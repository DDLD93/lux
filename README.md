# lux-api

HTTP wrapper around the [lux](https://github.com/iawia002/lux) video download
library. Submit a URL, poll for progress, then fetch the result from MinIO.

Job state is persisted in **Postgres**; finished files are uploaded to
**MinIO** (via the AWS SDK for Go v2) and served through short-lived
presigned URLs.

## Prerequisites

Matches the lux [prerequisites](https://github.com/iawia002/lux#prerequisites):

- Go 1.22+ (Docker image uses Go 1.25)
- [FFmpeg](https://ffmpeg.org) (bundled in the runtime image)

Pinned dependency: `github.com/iawia002/lux v0.24.1` (latest release at time
of writing). To install the upstream CLI separately:

```bash
go install github.com/iawia002/lux@latest
```

## Run

You need a reachable Postgres and MinIO (or any S3) instance. Point the API
at them via env vars (see [Configuration](#configuration)).

Build and run the image:

```bash
docker build -t lux-api:latest .

docker run --rm -p 8080:8080 \
  -e DATABASE_URL="postgres://lux:lux@host.docker.internal:5432/lux?sslmode=disable" \
  -e S3_ENDPOINT="http://host.docker.internal:9000" \
  -e S3_BUCKET=lux \
  -e S3_ACCESS_KEY=minioadmin \
  -e S3_SECRET_KEY=minioadmin \
  lux-api:latest
```

Or run locally without Docker:

```bash
go run ./cmd/server
```

- API: <http://localhost:8080>
- Swagger UI: <http://localhost:8080/swagger>
- OpenAPI spec: <http://localhost:8080/openapi.yaml>

## Quickstart

```bash
# 1. Start a job
ID=$(curl -s -X POST http://localhost:8080/jobs \
  -H 'content-type: application/json' \
  -d '{"url":"https://www.youtube.com/watch?v=dQw4w9WgXcQ"}' | jq -r .id)

# 2. Poll progress
curl -s http://localhost:8080/jobs/$ID | jq

# 3. When state == done, follow the redirect to the presigned MinIO URL
curl -OJL http://localhost:8080/jobs/$ID/file

# Metadata only (no download)
curl -s -X POST http://localhost:8080/extract \
  -H 'content-type: application/json' \
  -d '{"url":"https://www.youtube.com/watch?v=dQw4w9WgXcQ"}' | jq

# Cancel
curl -X DELETE http://localhost:8080/jobs/$ID
```

## Endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| POST   | /jobs            | Start a download job, returns the persisted Job |
| GET    | /jobs            | List jobs (newest first, capped at 200) |
| GET    | /jobs/{id}       | Poll job status / progress |
| DELETE | /jobs/{id}       | Cancel job; deletes the MinIO object if uploaded |
| GET    | /jobs/{id}/file  | 302 → presigned MinIO URL for the finished file |
| POST   | /extract         | Synchronous stream metadata |
| GET    | /healthz         | Liveness |
| GET    | /swagger         | Swagger UI |
| GET    | /openapi.yaml    | Raw OpenAPI 3 spec |

## Configuration

| Env var              | Default                                                 | Purpose                                  |
| -------------------- | ------------------------------------------------------- | ---------------------------------------- |
| `LUX_ADDR`           | `:8080`                                                 | HTTP listen address                      |
| `LUX_SERVER_URL`     | `http://localhost:8080`                                 | Public base URL shown in Swagger + `/`   |
| `LUX_SCRATCH_DIR`    | `os.TempDir()` (`/tmp/lux` in container)                | Per-job temp dir, deleted after upload   |
| `LUX_WORKERS`        | `4`                                                     | Concurrent download workers              |
| `DATABASE_URL`       | `postgres://lux:lux@postgres:5432/lux?sslmode=disable`  | Postgres DSN                             |
| `S3_ENDPOINT`        | `http://minio:9000`                                     | MinIO / S3 endpoint                      |
| `S3_BUCKET`          | `lux`                                                   | Object bucket (auto-created on boot)     |
| `S3_REGION`          | `us-east-1`                                             | Region label (required by SDK)           |
| `S3_ACCESS_KEY`      | `minioadmin`                                            | Access key                               |
| `S3_SECRET_KEY`      | `minioadmin`                                            | Secret key                               |
| `S3_USE_PATH_STYLE`  | `true`                                                  | Required for MinIO                       |
| `S3_PRESIGN_TTL`     | `5m`                                                    | Lifetime of presigned download URLs      |

## Notes

- Job rows live in the `jobs` table — schema is applied on boot from
  `internal/db/schema.sql`.
- Finished objects land at `jobs/{id}/{filename}` in the bucket.
- Cancellation is best-effort: lux's downloader does not accept a
  `context.Context`, so in-flight HTTP transfers run to natural completion
  before the job is marked `canceled`.
