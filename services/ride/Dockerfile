FROM golang:1.27-alpine AS builder

WORKDIR /src

COPY proto/go.mod proto/go.sum ./proto/
COPY services/ride/go.mod services/ride/go.sum ./services/ride/

WORKDIR /src/services/ride
RUN go mod download

COPY proto /src/proto
COPY services/ride /src/services/ride

RUN CGO_ENABLED=0 GOOS=linux go build -o /main ./cmd

FROM gcr.io/distroless/static-debian12

WORKDIR /

COPY --from=builder /main /main

EXPOSE 8080 50051

USER nonroot:nonroot

ENTRYPOINT ["/main"]