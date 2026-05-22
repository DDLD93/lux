package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/iawia002/lux/extractors"

	"github.com/umarj/lux-api/internal/jobs"
	"github.com/umarj/lux-api/internal/storage"
)

const maxBodyBytes = 64 * 1024

type handlers struct {
	store       *jobs.Store
	pool        *jobs.Pool
	storage     *storage.Client
	presignTTL  time.Duration
}

func decodeRequest(r *http.Request) (jobs.Request, error) {
	var req jobs.Request
	r.Body = http.MaxBytesReader(nil, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return req, fmt.Errorf("invalid JSON body: %w", err)
	}
	req.URL = strings.TrimSpace(req.URL)
	if req.URL == "" {
		return req, errors.New("field 'url' is required")
	}
	if !strings.HasPrefix(req.URL, "http://") && !strings.HasPrefix(req.URL, "https://") {
		return req, errors.New("field 'url' must be an http(s) URL")
	}
	return req, nil
}

func (h *handlers) createJob(w http.ResponseWriter, r *http.Request) {
	req, err := decodeRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// Resolve streams synchronously so extraction failures surface as a proper
	// HTTP error instead of a 202 followed by a silent async "failed" state.
	data, streamKey, err := jobs.Resolve(req)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	var bytesTotal int64
	if s := data.Streams[streamKey]; s != nil {
		bytesTotal = s.Size
	}
	j, err := h.store.Create(r.Context(), req, data.Title, data.Site, streamKey, bytesTotal)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.pool.Submit(j.ID)
	writeJSON(w, http.StatusAccepted, j)
}

func (h *handlers) listJobs(w http.ResponseWriter, r *http.Request) {
	list, err := h.store.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (h *handlers) getJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	j, err := h.store.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, jobs.ErrNotFound) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (h *handlers) cancelJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := h.pool.Cancel(r.Context(), id); err != nil {
		if errors.Is(err, jobs.ErrNotFound) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	j, err := h.store.Get(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]string{"id": id, "state": string(jobs.StateCanceled)})
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (h *handlers) downloadFile(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	j, err := h.store.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, jobs.ErrNotFound) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if j.State != jobs.StateDone {
		writeError(w, http.StatusConflict, "job not finished (state="+string(j.State)+")")
		return
	}
	if j.OutputObjectKey == "" {
		writeError(w, http.StatusInternalServerError, "no output object recorded for job")
		return
	}
	url, err := h.storage.PresignGet(r.Context(), j.OutputObjectKey, j.OutputFilename, h.presignTTL)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	http.Redirect(w, r, url, http.StatusFound)
}

func (h *handlers) extract(w http.ResponseWriter, r *http.Request) {
	req, err := decodeRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	data, err := safeExtract(req)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func safeExtract(req jobs.Request) (data []*extractors.Data, err error) {
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

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
