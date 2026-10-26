FROM golang:1.27-alpine AS builder

WORKDIR /src

COPY proto/go.mod proto/go.sum ./proto/
COPY tools/devconsole/go.mod tools/devconsole/go.sum ./tools/devconsole/

WORKDIR /src/tools/devconsole
RUN go mod download

COPY proto /src/proto
COPY tools/devconsole /src/tools/devconsole

RUN CGO_ENABLED=0 GOOS=linux go build -o /main .

FROM gcr.io/distroless/static-debian12

WORKDIR /

COPY --from=builder /main /main

EXPOSE 9000

USER nonroot:nonroot

ENTRYPOINT ["/main"]
