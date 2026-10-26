# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS builder

WORKDIR /src

COPY proto/go.mod proto/go.sum ./proto/
COPY pkg/go.mod pkg/go.sum ./pkg/
COPY services/location/go.mod services/location/go.sum ./services/location/

WORKDIR /src/services/location
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY proto /src/proto
COPY pkg /src/pkg
COPY services/location /src/services/location

# Module and build caches persist across builds and are shared by every service image, so dependencies
# compile once instead of on every build.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /main ./cmd

FROM gcr.io/distroless/static-debian12

WORKDIR /

COPY --from=builder /main /main

EXPOSE 8080 50051

USER nonroot:nonroot

ENTRYPOINT ["/main"]