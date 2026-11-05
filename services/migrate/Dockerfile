# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS builder
WORKDIR /src
COPY proto/go.mod proto/go.sum ./proto/
COPY pkg/go.mod pkg/go.sum ./pkg/
COPY services/user/go.mod services/user/go.sum ./services/user/
WORKDIR /src/services/user
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY proto /src/proto
COPY pkg /src/pkg
COPY services/user /src/services/user
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /migrate ./cmd/migrate
FROM gcr.io/distroless/static-debian12
COPY --from=builder /migrate /migrate
COPY services/user/migrations /migrations/user
COPY services/ride/migrations /migrations/ride
COPY services/matching/migrations /migrations/matching
COPY services/payment/migrations /migrations/payment
USER nonroot:nonroot
ENTRYPOINT ["/migrate"]
