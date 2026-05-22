package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"

	"github.com/umarj/lux-api/internal/db"
	"github.com/umarj/lux-api/internal/jobs"
	_ "github.com/umarj/lux-api/internal/lux" // register lux site extractors
	"github.com/umarj/lux-api/internal/server"
	"github.com/umarj/lux-api/internal/storage"
)

func main() {
	// Load .env if present. Existing env vars win, so container/runtime
	// values still override the file.
	for _, p := range []string{".env", "/app/.env"} {
		if _, err := os.Stat(p); err == nil {
			if err := godotenv.Load(p); err != nil {
				log.Printf("warning: load %s: %v", p, err)
			} else {
				log.Printf("loaded env from %s", p)
			}
			break
		}
	}

	ctx := context.Background()

	scratchDir := envOr("LUX_SCRATCH_DIR", os.TempDir())
	if err := os.MkdirAll(scratchDir, 0o755); err != nil {
		log.Fatalf("mkdir scratch dir: %v", err)
	}

	workers, _ := strconv.Atoi(envOr("LUX_WORKERS", "4"))
	if workers <= 0 {
		workers = 4
	}

	dsn := envOr("DATABASE_URL", "postgres://lux:lux@postgres:5432/lux?sslmode=disable")
	pgPool, err := db.Connect(ctx, dsn)
	if err != nil {
		log.Fatalf("postgres connect: %v", err)
	}
	defer pgPool.Close()
	if err := db.Migrate(ctx, pgPool); err != nil {
		log.Fatalf("postgres migrate: %v", err)
	}

	pathStyle, _ := strconv.ParseBool(envOr("S3_USE_PATH_STYLE", "true"))
	stCli, err := storage.New(ctx, storage.Config{
		Endpoint:     envOr("S3_ENDPOINT", "http://minio:9000"),
		Region:       envOr("S3_REGION", "us-east-1"),
		AccessKey:    envOr("S3_ACCESS_KEY", "minioadmin"),
		SecretKey:    envOr("S3_SECRET_KEY", "minioadmin"),
		Bucket:       envOr("S3_BUCKET", "lux"),
		UsePathStyle: pathStyle,
	})
	if err != nil {
		log.Fatalf("storage init: %v", err)
	}
	if err := stCli.EnsureBucket(ctx); err != nil {
		log.Fatalf("storage ensure bucket: %v", err)
	}

	presignTTL, err := time.ParseDuration(envOr("S3_PRESIGN_TTL", "5m"))
	if err != nil {
		log.Fatalf("parse S3_PRESIGN_TTL: %v", err)
	}

	store := jobs.NewStore(pgPool)
	pool := jobs.NewPool(store, stCli, scratchDir, workers)
	pool.Start()
	defer pool.Stop()

	addr := envOr("LUX_ADDR", ":8080")
	serverURL := envOr("LUX_SERVER_URL", "http://localhost:8080")
	srv := &http.Server{
		Addr:              addr,
		Handler:           server.New(store, pool, stCli, presignTTL, serverURL),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("lux-api listening on %s (scratch=%s, workers=%d, bucket=%s)", addr, scratchDir, workers, stCli.Bucket())
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
