package jobs

import (
	"context"
	"sync"
	"time"

	"github.com/iawia002/lux/extractors"
)

type State string

const (
	StateQueued      State = "queued"
	StateExtracting  State = "extracting"
	StateDownloading State = "downloading"
	StateDone        State = "done"
	StateFailed      State = "failed"
	StateCanceled    State = "canceled"
)

type Request struct {
	URL      string `json:"url"`
	Stream   string `json:"stream,omitempty"`
	Playlist bool   `json:"playlist,omitempty"`
	Cookie   string `json:"cookie,omitempty"`
}

type Job struct {
	ID              string    `json:"id"`
	URL             string    `json:"url"`
	State           State     `json:"state"`
	Percent         float64   `json:"percent"`
	BytesDownloaded int64     `json:"bytes_downloaded"`
	BytesTotal      int64     `json:"bytes_total"`
	Title           string    `json:"title,omitempty"`
	Site            string    `json:"site,omitempty"`
	OutputFilename  string    `json:"output_filename,omitempty"`
	OutputDir       string    `json:"-"`
	Error           string    `json:"error,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`

	Req       Request            `json:"-"`
	Data      *extractors.Data   `json:"-"`
	StreamKey string             `json:"-"`
	mu        sync.Mutex         `json:"-"`
	ctx       context.Context    `json:"-"`
	cancel    context.CancelFunc `json:"-"`
	done      chan struct{}      `json:"-"`
}

func (j *Job) Snapshot() Job {
	j.mu.Lock()
	defer j.mu.Unlock()
	return Job{
		ID:              j.ID,
		URL:             j.URL,
		State:           j.State,
		Percent:         j.Percent,
		BytesDownloaded: j.BytesDownloaded,
		BytesTotal:      j.BytesTotal,
		Title:           j.Title,
		Site:            j.Site,
		OutputFilename:  j.OutputFilename,
		Error:           j.Error,
		CreatedAt:       j.CreatedAt,
		UpdatedAt:       j.UpdatedAt,
	}
}

func (j *Job) update(fn func(*Job)) {
	j.mu.Lock()
	defer j.mu.Unlock()
	fn(j)
	j.UpdatedAt = time.Now().UTC()
}
