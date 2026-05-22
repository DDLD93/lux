package jobs

import "time"

type State string

const (
	StateQueued      State = "queued"
	StateExtracting  State = "extracting"
	StateDownloading State = "downloading"
	StateUploading   State = "uploading"
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
	ID               string    `json:"id"`
	URL              string    `json:"url"`
	State            State     `json:"state"`
	Percent          float64   `json:"percent"`
	BytesDownloaded  int64     `json:"bytes_downloaded"`
	BytesTotal       int64     `json:"bytes_total"`
	Title            string    `json:"title,omitempty"`
	Site             string    `json:"site,omitempty"`
	StreamKey        string    `json:"stream,omitempty"`
	OutputFilename   string    `json:"output_filename,omitempty"`
	OutputObjectKey  string    `json:"output_object_key,omitempty"`
	Error            string    `json:"error,omitempty"`
	Playlist         bool      `json:"playlist,omitempty"`
	Cookie           string    `json:"-"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func (j Job) Request() Request {
	return Request{
		URL:      j.URL,
		Stream:   j.StreamKey,
		Playlist: j.Playlist,
		Cookie:   j.Cookie,
	}
}
