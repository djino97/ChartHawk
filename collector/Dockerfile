# Multi-stage build (см. 8. Развёртывание аналитики).
FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /collector ./cmd/collector

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /collector /collector
ENTRYPOINT ["/collector"]
