package server

import (
	"bytes"
	"embed"
	"io/fs"
	"net/http"
	"strings"
	"text/template"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/swaggest/swgui/v5emb"

	"github.com/umarj/lux-api/internal/jobs"
)

//go:embed openapi.yaml index.html
var assetsFS embed.FS

func New(store *jobs.Store, pool *jobs.Pool, outDir, serverURL string) http.Handler {
	h := &handlers{store: store, pool: pool, outDir: outDir}

	specRaw, _ := fs.ReadFile(assetsFS, "openapi.yaml")
	spec := []byte(strings.ReplaceAll(string(specRaw), "${LUX_SERVER_URL}", serverURL))

	indexRaw, _ := fs.ReadFile(assetsFS, "index.html")
	indexTpl := template.Must(template.New("index").Parse(string(indexRaw)))
	var indexBuf bytes.Buffer
	_ = indexTpl.Execute(&indexBuf, map[string]string{"ServerURL": serverURL})
	indexHTML := indexBuf.Bytes()

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	})
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	})

	r.Post("/jobs", h.createJob)
	r.Get("/jobs", h.listJobs)
	r.Get("/jobs/{id}", h.getJob)
	r.Delete("/jobs/{id}", h.cancelJob)
	r.Get("/jobs/{id}/file", h.downloadFile)
	r.Post("/extract", h.extract)

	r.Get("/openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(spec)
	})
	r.Mount("/swagger", v5emb.New("lux-api", "/openapi.yaml", "/swagger"))

	return r
}
