# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS builder

WORKDIR /src

COPY proto/go.mod proto/go.sum ./proto/
COPY services/ride/go.mod services/ride/go.sum ./services/ride/

WORKDIR /src/services/ride
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY proto /src/proto
COPY services/ride /src/services/ride

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