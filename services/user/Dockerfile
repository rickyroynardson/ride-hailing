FROM golang:1.27-alpine AS builder

WORKDIR /src

COPY proto/go.mod proto/go.sum ./proto/
COPY services/user/go.mod services/user/go.sum ./services/user/

WORKDIR /src/services/user
RUN go mod download

COPY proto /src/proto
COPY services/user /src/services/user

RUN CGO_ENABLED=0 GOOS=linux go build -o /main ./cmd

FROM gcr.io/distroless/static-debian12

WORKDIR /

COPY --from=builder /main /main

EXPOSE 8080 50051

USER nonroot:nonroot

ENTRYPOINT ["/main"]