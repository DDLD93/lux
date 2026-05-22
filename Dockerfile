FROM golang:1.26.3-alpine AS build  
WORKDIR /src
RUN apk add --no-cache git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/lux-api ./cmd/server

FROM alpine:3.20
RUN apk add --no-cache ffmpeg ca-certificates tini && \
    addgroup -S lux && adduser -S -G lux lux && \
    mkdir -p /tmp/lux && chown -R lux:lux /tmp/lux
COPY --from=build /out/lux-api /usr/local/bin/lux-api

# Server
ENV LUX_ADDR=:8080 \
    LUX_SERVER_URL=http://localhost:8080 \
    LUX_SCRATCH_DIR=/tmp/lux \
    LUX_WORKERS=4
# Postgres (override at runtime)
ENV DATABASE_URL=postgres://lux:lux@postgres:5432/lux?sslmode=disable
# MinIO / S3 (override at runtime)
ENV S3_ENDPOINT=http://minio:9000 \
    S3_BUCKET=lux \
    S3_REGION=us-east-1 \
    S3_ACCESS_KEY=minioadmin \
    S3_SECRET_KEY=minioadmin \
    S3_USE_PATH_STYLE=true \
    S3_PRESIGN_TTL=5m

USER lux
EXPOSE 8080
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/lux-api"]
