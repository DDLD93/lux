package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"

	"github.com/go-chi/chi/v5"
	"github.com/iawia002/lux/extractors"

	"github.com/umarj/lux-api/internal/jobs"
)

type handlers struct {
	store  *jobs.Store
	pool   *jobs.Pool
	outDir string
}

func (h *handlers) createJob(w http.ResponseWriter, r *http.Request) {
	var req jobs.Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.URL == "" {
		writeError(w, http.StatusBadRequest, "field 'url' is required")
		return
	}
	j := h.store.Create(req)
	h.pool.Submit(j.ID)
	writeJSON(w, http.StatusAccepted, map[string]string{"id": j.ID})
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
	w.WriteHeader(http.StatusNoContent)
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
		writeError(w, http.StatusInternalServerError, "no output file recorded")
		return
	}
	path := filepath.Join(h.outDir, snap.ID, snap.OutputFilename)
	w.Header().Set("Content-Disposition", `attachment; filename="`+snap.OutputFilename+`"`)
	http.ServeFile(w, r, path)
}

func (h *handlers) extract(w http.ResponseWriter, r *http.Request) {
	var req jobs.Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.URL == "" {
		writeError(w, http.StatusBadRequest, "field 'url' is required")
		return
	}
	data, err := extractors.Extract(req.URL, extractors.Options{
		Playlist: req.Playlist,
		Cookie:   req.Cookie,
	})
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
