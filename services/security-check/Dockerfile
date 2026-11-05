# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS builder
WORKDIR /src/pkg
COPY pkg/go.mod pkg/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY pkg /src/pkg
# Loopback tests run inside Docker; all temporary key material disappears with this build step.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go test -count=1 ./lifecycle ./workloadtls ./securitysetup ./jwtauth ./grpcserver && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /check ./cmd/securitycheck
FROM gcr.io/distroless/static-debian12
COPY --from=builder /check /check
USER nonroot:nonroot
ENTRYPOINT ["/check"]
