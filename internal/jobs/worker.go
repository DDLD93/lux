package jobs

import (
	"context"
	"fmt"
	"log"
	"mime"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/iawia002/lux/downloader"
	"github.com/iawia002/lux/extractors"

	"github.com/umarj/lux-api/internal/storage"
)

type Pool struct {
	store    *Store
	storage  *storage.Client
	scratch  string
	queue    chan string
	workers  int
	stopCh   chan struct{}
	cancelMu sync.Mutex
	cancels  map[string]context.CancelFunc
}

func NewPool(store *Store, st *storage.Client, scratchDir string, workers int) *Pool {
	return &Pool{
		store:   store,
		storage: st,
		scratch: scratchDir,
		queue:   make(chan string, 1024),
		workers: workers,
		stopCh:  make(chan struct{}),
		cancels: make(map[string]context.CancelFunc),
	}
}

func (p *Pool) Start() {
	for i := 0; i < p.workers; i++ {
		go p.loop()
	}
}

func (p *Pool) Stop() { close(p.stopCh) }

func (p *Pool) Submit(id string) { p.queue <- id }

// Cancel signals an in-flight worker to stop. If the job is not running on
// this instance, it just marks the row canceled if not yet terminal.
func (p *Pool) Cancel(ctx context.Context, id string) error {
	j, err := p.store.Get(ctx, id)
	if err != nil {
		return err
	}

	p.cancelMu.Lock()
	cancel, running := p.cancels[id]
	p.cancelMu.Unlock()
	if running {
		cancel()
	}

	// Best-effort: clean up the MinIO object if the file was already uploaded.
	if j.OutputObjectKey != "" && p.storage != nil {
		_ = p.storage.Delete(ctx, j.OutputObjectKey)
	}

	_, err = p.store.Update(ctx, id, func(j *Job) {
		if j.State != StateDone && j.State != StateFailed {
			j.State = StateCanceled
		}
		j.OutputObjectKey = ""
	})
	return err
}

func (p *Pool) registerCancel(id string, cancel context.CancelFunc) {
	p.cancelMu.Lock()
	p.cancels[id] = cancel
	p.cancelMu.Unlock()
}

func (p *Pool) clearCancel(id string) {
	p.cancelMu.Lock()
	delete(p.cancels, id)
	p.cancelMu.Unlock()
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
	ctx, cancel := context.WithCancel(context.Background())
	p.registerCancel(id, cancel)
	defer func() {
		cancel()
		p.clearCancel(id)
	}()

	defer func() {
		if r := recover(); r != nil {
			p.fail(ctx, id, fmt.Errorf("panic: %v", r))
		}
	}()

	j, err := p.store.Get(ctx, id)
	if err != nil {
		log.Printf("job %s: load: %v", id, err)
		return
	}

	jobDir := filepath.Join(p.scratch, j.ID)
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		p.fail(ctx, id, fmt.Errorf("mkdir job dir: %w", err))
		return
	}
	defer os.RemoveAll(jobDir)

	// Re-resolve streams in the worker — extractors.Data is not safely
	// serializable and the createJob handler only persists derived metadata.
	if _, err := p.store.Update(ctx, id, func(j *Job) { j.State = StateExtracting }); err != nil {
		log.Printf("job %s: update extracting: %v", id, err)
	}

	data, streamKey, err := Resolve(j.Request())
	if err != nil {
		p.fail(ctx, id, err)
		return
	}

	if _, err := p.store.Update(ctx, id, func(j *Job) {
		j.State = StateDownloading
		j.Title = data.Title
		j.Site = data.Site
		j.StreamKey = streamKey
		if s := data.Streams[streamKey]; s != nil {
			j.BytesTotal = s.Size
		}
	}); err != nil {
		log.Printf("job %s: update downloading: %v", id, err)
	}

	progressCtx, stopProgress := context.WithCancel(ctx)
	go p.trackProgress(progressCtx, id, jobDir)

	dl := downloader.New(downloader.Options{
		OutputPath:   jobDir,
		OutputName:   data.Title,
		Stream:       streamKey,
		Refer:        data.URL,
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
		doneCh <- dl.Download(data)
	}()

	select {
	case err = <-doneCh:
	case <-ctx.Done():
		stopProgress()
		<-doneCh
		_, _ = p.store.Update(ctx, id, func(j *Job) { j.State = StateCanceled })
		return
	}
	stopProgress()

	if err != nil {
		p.fail(context.Background(), id, fmt.Errorf("download: %w", err))
		return
	}

	outName := findOutput(jobDir)
	if outName == "" {
		p.fail(context.Background(), id, fmt.Errorf("no output file produced"))
		return
	}
	outPath := filepath.Join(jobDir, outName)

	if _, err := p.store.Update(ctx, id, func(j *Job) { j.State = StateUploading }); err != nil {
		log.Printf("job %s: update uploading: %v", id, err)
	}

	objectKey := "jobs/" + id + "/" + outName
	contentType := mime.TypeByExtension(filepath.Ext(outName))
	uploadCtx, cancelUpload := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancelUpload()
	if err := p.storage.UploadFile(uploadCtx, objectKey, outPath, contentType); err != nil {
		p.fail(context.Background(), id, fmt.Errorf("upload: %w", err))
		return
	}

	info, _ := os.Stat(outPath)
	var size int64
	if info != nil {
		size = info.Size()
	}

	if _, err := p.store.Update(context.Background(), id, func(j *Job) {
		j.State = StateDone
		j.Percent = 100
		j.OutputFilename = outName
		j.OutputObjectKey = objectKey
		if size > 0 {
			j.BytesDownloaded = size
			if j.BytesTotal == 0 {
				j.BytesTotal = size
			}
		}
	}); err != nil {
		log.Printf("job %s: update done: %v", id, err)
		return
	}
	log.Printf("job %s done: %s -> %s", id, outName, objectKey)
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

func (p *Pool) fail(ctx context.Context, id string, err error) {
	log.Printf("job %s failed: %v", id, err)
	_, uerr := p.store.Update(ctx, id, func(j *Job) {
		j.State = StateFailed
		j.Error = err.Error()
	})
	if uerr != nil {
		log.Printf("job %s: failed to record failure: %v", id, uerr)
	}
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

func (p *Pool) trackProgress(ctx context.Context, id, dir string) {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n := dirSize(dir)
			_, _ = p.store.Update(ctx, id, func(j *Job) {
				j.BytesDownloaded = n
				if j.BytesTotal > 0 {
					pct := float64(n) / float64(j.BytesTotal) * 100
					if pct > 99.9 {
						pct = 99.9
					}
					j.Percent = pct
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
