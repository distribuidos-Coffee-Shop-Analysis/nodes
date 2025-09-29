# Build stage
FROM golang:1.21-alpine AS builder

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code maintaining directory structure
COPY main.go ./
COPY common/ ./common/
COPY protocol/ ./protocol/
COPY middleware/ ./middleware/
COPY node/ ./node/

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .

# Final stage
FROM alpine:latest


# Create app directory
WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/main .
# Copy configuration file
COPY config.ini .


# Expose port (if needed for health checks)
EXPOSE 12345

# Run the binary
ENTRYPOINT ["./main"]
