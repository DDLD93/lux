package server

import (
	"embed"
	"io/fs"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/swaggest/swgui/v5emb"

	"github.com/umarj/lux-api/internal/jobs"
)

//go:embed openapi.yaml
var openapiFS embed.FS

func New(store *jobs.Store, pool *jobs.Pool, outDir string) http.Handler {
	h := &handlers{store: store, pool: pool, outDir: outDir}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	})

	r.Post("/jobs", h.createJob)
	r.Get("/jobs", h.listJobs)
	r.Get("/jobs/{id}", h.getJob)
	r.Delete("/jobs/{id}", h.cancelJob)
	r.Get("/jobs/{id}/file", h.downloadFile)
	r.Post("/extract", h.extract)

	// OpenAPI spec + Swagger UI.
	r.Get("/openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		b, _ := fs.ReadFile(openapiFS, "openapi.yaml")
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(b)
	})
	r.Mount("/swagger", v5emb.New("lux-api", "/openapi.yaml", "/swagger"))

	return r
}
