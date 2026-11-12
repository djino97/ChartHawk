# Multi-stage build (см. 8. Развёртывание аналитики).
FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /collector ./cmd/collector

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata
# *.tinkoff.ru подписан Russian Trusted Root CA (Минцифры). Этого корня нет
# в наборе Mozilla, который ставит ca-certificates, и gRPC падает с
# "x509: certificate signed by unknown authority".
COPY certs/russian_trusted_root_ca.crt /usr/local/share/ca-certificates/russian_trusted_root_ca.crt
RUN update-ca-certificates
COPY --from=builder /collector /collector
ENTRYPOINT ["/collector"]
