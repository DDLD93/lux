package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/umarj/lux-api/internal/jobs"
	"github.com/umarj/lux-api/internal/server"
)

func main() {
	outDir := envOr("LUX_OUTPUT_DIR", "/data/downloads")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		log.Fatalf("mkdir output dir: %v", err)
	}

	workers, _ := strconv.Atoi(envOr("LUX_WORKERS", "4"))
	if workers <= 0 {
		workers = 4
	}

	store := jobs.NewStore()
	pool := jobs.NewPool(store, outDir, workers)
	pool.Start()
	defer pool.Stop()

	addr := envOr("LUX_ADDR", ":8080")
	srv := &http.Server{
		Addr:              addr,
		Handler:           server.New(store, pool, outDir),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("lux-api listening on %s (output=%s, workers=%d)", addr, outDir, workers)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
