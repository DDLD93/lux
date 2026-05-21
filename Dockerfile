FROM golang:1.22-alpine AS build
WORKDIR /src
RUN apk add --no-cache git
COPY go.mod ./
COPY go.sum* ./
RUN go mod download || true
COPY . .
RUN go mod tidy && \
    CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/lux-api ./cmd/server

FROM alpine:3.20
RUN apk add --no-cache ffmpeg ca-certificates && \
    mkdir -p /data/downloads
COPY --from=build /out/lux-api /usr/local/bin/lux-api
ENV LUX_OUTPUT_DIR=/data/downloads \
    LUX_WORKERS=4 \
    LUX_ADDR=:8080 \
    LUX_SERVER_URL=http://localhost:8080
EXPOSE 8080
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/lux-api"]
