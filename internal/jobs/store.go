package jobs

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

var ErrNotFound = errors.New("job not found")

type Store struct {
	mu   sync.RWMutex
	jobs map[string]*Job
}

func NewStore() *Store {
	return &Store{jobs: make(map[string]*Job)}
}

func (s *Store) Create(req Request) *Job {
	ctx, cancel := context.WithCancel(context.Background())
	j := &Job{
		ID:        uuid.NewString(),
		URL:       req.URL,
		State:     StateQueued,
		Req:       req,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
		ctx:       ctx,
		cancel:    cancel,
		done:      make(chan struct{}),
	}
	s.mu.Lock()
	s.jobs[j.ID] = j
	s.mu.Unlock()
	return j
}

func (s *Store) Get(id string) (*Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	return j, nil
}

func (s *Store) Snapshot(id string) (Job, error) {
	j, err := s.Get(id)
	if err != nil {
		return Job{}, err
	}
	return j.snapshot(), nil
}

func (s *Store) List() []Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		out = append(out, j.snapshot())
	}
	return out
}

func (s *Store) Delete(id string) {
	s.mu.Lock()
	delete(s.jobs, id)
	s.mu.Unlock()
}
