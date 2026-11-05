# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS builder
WORKDIR /src/pkg
COPY pkg/go.mod pkg/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY pkg /src/pkg
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /setup ./cmd/securitysetup
FROM gcr.io/distroless/static-debian12
COPY --from=builder /setup /setup
# Provisioning alone runs as root to assign volume ownership to the nonroot application UID.
USER 0:0
ENTRYPOINT ["/setup"]
