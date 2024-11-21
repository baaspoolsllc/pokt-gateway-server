# Stage 1: Build stage
FROM golang:1.21.4-alpine AS builder

WORKDIR /app

# Copy only the necessary files for Go module dependency resolution
COPY go.mod go.sum ./

# Download Go dependencies
RUN go mod download

# Copy the entire application
COPY . .

# Build the Go application with optimizations
RUN go build -o main cmd/gateway_server/main.go

# Stage 2: Runtime stage
FROM scratch

WORKDIR /app

# Copy only the necessary files from the builder stage
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/main .

# Set default value for port exposed
ENV HTTP_SERVER_PORT=8080

EXPOSE $HTTP_SERVER_PORT

CMD ["/app/main"]
