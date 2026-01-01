# Build Stage
FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o sentinel-server ./cmd/sentinel-server

# Runtime Stage
FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/sentinel-server .

# Create data directory
RUN mkdir -p /app/data

# Expose gRPC port
EXPOSE 9092

# Run server
CMD ["./sentinel-server", "--port", "9092", "--data", "/app/data"]
