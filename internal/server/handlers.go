package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/iawia002/lux/extractors"

	"github.com/umarj/lux-api/internal/jobs"
)

const maxBodyBytes = 64 * 1024

type handlers struct {
	store  *jobs.Store
	pool   *jobs.Pool
	outDir string
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
	j := h.store.Create(req)
	j.Data = data
	j.StreamKey = streamKey
	j.Title = data.Title
	j.Site = data.Site
	if s := data.Streams[streamKey]; s != nil {
		j.BytesTotal = s.Size
	}
	h.pool.Submit(j.ID)
	writeJSON(w, http.StatusAccepted, j.Snapshot())
}

func (h *handlers) listJobs(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.store.List())
}

func (h *handlers) getJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	snap, err := h.store.Snapshot(id)
	if err != nil {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (h *handlers) cancelJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := h.pool.Cancel(id); err != nil {
		if errors.Is(err, jobs.ErrNotFound) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	snap, err := h.store.Snapshot(id)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]string{"id": id, "state": string(jobs.StateCanceled)})
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (h *handlers) downloadFile(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	snap, err := h.store.Snapshot(id)
	if err != nil {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if snap.State != jobs.StateDone {
		writeError(w, http.StatusConflict, "job not finished (state="+string(snap.State)+")")
		return
	}
	if snap.OutputFilename == "" {
		writeError(w, http.StatusInternalServerError, "no output file recorded for job")
		return
	}
	path := filepath.Join(h.outDir, snap.ID, snap.OutputFilename)
	if _, err := os.Stat(path); err != nil {
		writeError(w, http.StatusGone, "output file no longer available")
		return
	}
	w.Header().Set("Content-Disposition", `attachment; filename="`+snap.OutputFilename+`"`)
	http.ServeFile(w, r, path)
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
