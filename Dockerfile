# ---- Build Stage ----
FROM golang:1.23.11-alpine AS builder

RUN apk add --no-cache gcc musl-dev ca-certificates

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-w -s" -o grapthway .

# ---- Final Stage ----
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder /app/grapthway /app/grapthway

# Create directory for certificates
RUN mkdir -p /data/certs && chmod 700 /data/certs

# Expose HTTP (ACME), HTTPS, and P2P
EXPOSE 80 443 5000

CMD ["/app/grapthway"]
