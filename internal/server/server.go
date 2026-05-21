package server

import (
	"bytes"
	"embed"
	"io/fs"
	"log"
	"net/http"
	"runtime/debug"
	"strings"
	"text/template"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/swaggest/swgui/v5emb"

	"github.com/umarj/lux-api/internal/jobs"
)

//go:embed openapi.yaml index.html ui.html
var assetsFS embed.FS

func New(store *jobs.Store, pool *jobs.Pool, outDir, serverURL string) http.Handler {
	h := &handlers{store: store, pool: pool, outDir: outDir}

	specRaw, _ := fs.ReadFile(assetsFS, "openapi.yaml")
	spec := []byte(strings.ReplaceAll(string(specRaw), "${LUX_SERVER_URL}", serverURL))

	indexHTML := renderTpl("index.html", map[string]string{"ServerURL": serverURL})
	uiHTML := renderTpl("ui.html", map[string]string{"ServerURL": serverURL})

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(jsonRecoverer)

	r.NotFound(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "not found")
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	})

	r.Get("/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	})
	r.Get("/ui", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(uiHTML)
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

func renderTpl(name string, data any) []byte {
	raw, _ := fs.ReadFile(assetsFS, name)
	tpl := template.Must(template.New(name).Parse(string(raw)))
	var buf bytes.Buffer
	_ = tpl.Execute(&buf, data)
	return buf.Bytes()
}

// jsonRecoverer mirrors chi's middleware.Recoverer but emits a JSON envelope.
func jsonRecoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil && rec != http.ErrAbortHandler {
				log.Printf("panic in %s %s: %v\n%s", r.Method, r.URL.Path, rec, debug.Stack())
				if w.Header().Get("Content-Type") == "" {
					writeError(w, http.StatusInternalServerError, "internal server error")
				}
			}
		}()
		next.ServeHTTP(w, r)
	})
}
