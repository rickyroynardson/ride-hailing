# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS builder
WORKDIR /src/pkg
COPY pkg/go.mod pkg/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY pkg /src/pkg
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go test -count=1 ./lifecycle && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /drill ./cmd/deploymentcheck && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /probe ./cmd/probe
FROM gcr.io/distroless/static-debian12
COPY --from=builder /drill /drill
COPY --from=builder /probe /probe
USER nonroot:nonroot
ENTRYPOINT ["/drill"]
