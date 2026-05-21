# lux-api

HTTP wrapper around the [lux](https://github.com/iawia002/lux) video download
library. Submit a URL, poll for progress, then download the result.

## Run

```bash
docker compose up --build
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

# 3. When state == done, fetch the file
curl -OJ http://localhost:8080/jobs/$ID/file

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
| POST   | /jobs            | Start a download job, returns `{id}` |
| GET    | /jobs            | List jobs |
| GET    | /jobs/{id}       | Poll job status / progress |
| DELETE | /jobs/{id}       | Cancel job and remove partials |
| GET    | /jobs/{id}/file  | Download the finished file |
| POST   | /extract         | Synchronous stream metadata |
| GET    | /healthz         | Liveness |
| GET    | /swagger         | Swagger UI |
| GET    | /openapi.yaml    | Raw OpenAPI 3 spec |

## Configuration

| Env var          | Default          | Purpose                       |
| ---------------- | ---------------- | ----------------------------- |
| `LUX_ADDR`       | `:8080`                  | HTTP listen address                            |
| `LUX_SERVER_URL` | `http://localhost:8080`  | Public base URL shown in Swagger + `/` UI      |
| `LUX_OUTPUT_DIR` | `/data/downloads`        | Where finished files land                      |
| `LUX_WORKERS`    | `4`                      | Concurrent download workers                    |

## Notes

- Job state is in-memory; restarting the container forgets jobs.
- Progress is derived by polling the on-disk job directory every 500ms — lux's
  downloader does not expose a callback.
- Cancellation is best-effort: lux's downloader does not accept a
  `context.Context`, so in-flight HTTP transfers run to natural completion
  before the job is marked `canceled`.
