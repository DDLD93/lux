package jobs

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/iawia002/lux/downloader"
	"github.com/iawia002/lux/extractors"
)

type Pool struct {
	store   *Store
	baseDir string
	queue   chan string
	workers int
	stopCh  chan struct{}
}

func NewPool(store *Store, baseDir string, workers int) *Pool {
	return &Pool{
		store:   store,
		baseDir: baseDir,
		queue:   make(chan string, 1024),
		workers: workers,
		stopCh:  make(chan struct{}),
	}
}

func (p *Pool) Start() {
	for i := 0; i < p.workers; i++ {
		go p.loop()
	}
}

func (p *Pool) Stop() { close(p.stopCh) }

func (p *Pool) Submit(id string) { p.queue <- id }

func (p *Pool) Cancel(id string) error {
	j, err := p.store.Get(id)
	if err != nil {
		return err
	}
	j.cancel()
	// Best-effort wait for the worker to exit if it had started.
	select {
	case <-j.done:
	case <-time.After(3 * time.Second):
	}
	j.update(func(j *Job) {
		if j.State != StateDone && j.State != StateFailed {
			j.State = StateCanceled
		}
	})
	if j.OutputDir != "" {
		_ = os.RemoveAll(j.OutputDir)
	}
	return nil
}

func (p *Pool) loop() {
	for {
		select {
		case <-p.stopCh:
			return
		case id := <-p.queue:
			p.run(id)
		}
	}
}

func (p *Pool) run(id string) {
	j, err := p.store.Get(id)
	if err != nil {
		return
	}
	defer close(j.done)
	defer func() {
		if r := recover(); r != nil {
			fail(j, fmt.Errorf("panic: %v", r))
		}
	}()

	if j.ctx.Err() != nil {
		j.update(func(j *Job) { j.State = StateCanceled })
		return
	}

	jobDir := filepath.Join(p.baseDir, j.ID)
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		fail(j, fmt.Errorf("mkdir job dir: %w", err))
		return
	}
	j.update(func(j *Job) { j.OutputDir = jobDir })

	d := j.Data
	streamKey := j.StreamKey
	if d == nil {
		// Fallback: extraction wasn't done up-front. This path also handles
		// any future callers that submit jobs without pre-resolving.
		j.update(func(j *Job) { j.State = StateExtracting })
		data, key, err := Resolve(j.Req)
		if err != nil {
			fail(j, err)
			return
		}
		d = data
		streamKey = key
		j.update(func(j *Job) {
			j.Data = d
			j.StreamKey = key
			j.Title = d.Title
			j.Site = d.Site
			if s := d.Streams[key]; s != nil {
				j.BytesTotal = s.Size
			}
		})
	}

	j.update(func(j *Job) { j.State = StateDownloading })

	progressCtx, stopProgress := context.WithCancel(j.ctx)
	go trackProgress(progressCtx, j, jobDir)

	dl := downloader.New(downloader.Options{
		OutputPath:   jobDir,
		OutputName:   d.Title,
		Stream:       streamKey,
		Refer:        d.URL,
		Silent:       true,
		MultiThread:  true,
		ThreadNumber: 4,
		RetryTimes:   3,
		ChunkSizeMB:  1,
	})

	doneCh := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				doneCh <- fmt.Errorf("download panic: %v", r)
			}
		}()
		doneCh <- dl.Download(d)
	}()

	select {
	case err = <-doneCh:
	case <-j.ctx.Done():
		stopProgress()
		// Wait briefly for the downloader to unwind; lux doesn't accept a
		// context, so the in-flight HTTP transfer will end on its own.
		<-doneCh
		j.update(func(j *Job) { j.State = StateCanceled })
		return
	}
	stopProgress()

	if err != nil {
		fail(j, fmt.Errorf("download: %w", err))
		return
	}

	outName := findOutput(jobDir)
	j.update(func(j *Job) {
		j.State = StateDone
		j.Percent = 100
		j.OutputFilename = outName
		if j.BytesTotal == 0 {
			j.BytesTotal = j.BytesDownloaded
		}
	})
	log.Printf("job %s done: %s", j.ID, outName)
}

func safeExtract(req Request) (data []*extractors.Data, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("extract panic: %v", r)
		}
	}()
	return extractors.Extract(req.URL, extractors.Options{
		Playlist: req.Playlist,
		Cookie:   req.Cookie,
	})
}

// Resolve runs lux's extractor synchronously and returns the chosen Data + stream key.
// Surfaces every failure mode as an error so callers can return a proper HTTP response
// instead of accepting an unrunnable job.
func Resolve(req Request) (*extractors.Data, string, error) {
	data, err := safeExtract(req)
	if err != nil {
		return nil, "", fmt.Errorf("extract: %w", err)
	}
	if len(data) == 0 || data[0] == nil {
		return nil, "", fmt.Errorf("no streams extracted (site likely unsupported)")
	}
	d := data[0]
	if d.Err != nil {
		return nil, "", fmt.Errorf("extract: %w", d.Err)
	}
	if len(d.Streams) == 0 {
		return nil, "", fmt.Errorf("no streams returned for %q", d.URL)
	}
	key := req.Stream
	if key == "" || d.Streams[key] == nil {
		key = bestStream(d)
	}
	if s := d.Streams[key]; s == nil {
		return nil, "", fmt.Errorf("stream %q not found", key)
	}
	return d, key, nil
}

func fail(j *Job, err error) {
	log.Printf("job %s failed: %v", j.ID, err)
	j.update(func(j *Job) {
		j.State = StateFailed
		j.Error = err.Error()
	})
}

func bestStream(d *extractors.Data) string {
	var (
		bestKey  string
		bestSize int64 = -1
	)
	for k, s := range d.Streams {
		if s.Size > bestSize {
			bestSize = s.Size
			bestKey = k
		}
	}
	return bestKey
}

func trackProgress(ctx context.Context, j *Job, dir string) {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n := dirSize(dir)
			j.update(func(j *Job) {
				j.BytesDownloaded = n
				if j.BytesTotal > 0 {
					p := float64(n) / float64(j.BytesTotal) * 100
					if p > 99.9 {
						p = 99.9
					}
					j.Percent = p
				}
			})
		}
	}
}

func dirSize(dir string) int64 {
	var total int64
	_ = filepath.WalkDir(dir, func(path string, e os.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return nil
		}
		if info, err := e.Info(); err == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}

// findOutput returns the largest non-part file in the job directory — lux
// leaves the muxed result alongside any leftover chunks.
func findOutput(dir string) string {
	var (
		best     string
		bestSize int64
	)
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if filepath.Ext(name) == ".download" {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.Size() > bestSize {
			bestSize = info.Size()
			best = name
		}
	}
	return best
}
