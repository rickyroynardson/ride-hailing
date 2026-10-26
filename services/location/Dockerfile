FROM golang:1.27-alpine AS builder

WORKDIR /src

COPY proto/go.mod proto/go.sum ./proto/
COPY services/location/go.mod services/location/go.sum ./services/location/

WORKDIR /src/services/location
RUN go mod download

COPY proto /src/proto
COPY services/location /src/services/location

RUN CGO_ENABLED=0 GOOS=linux go build -o /main ./cmd

FROM gcr.io/distroless/static-debian12

WORKDIR /

COPY --from=builder /main /main

EXPOSE 8080 50051

USER nonroot:nonroot

ENTRYPOINT ["/main"]