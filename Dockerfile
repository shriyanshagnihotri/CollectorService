# Use golang as the base image
FROM golang:1.17-alpine AS builder

# Set the working directory
WORKDIR /app

# Copy the server code into the container
COPY server.go go.mod go.sum /app/

# Build the server binary
RUN go build -o server

# Create a new stage with a minimal image
FROM alpine:latest

# Set the working directory
WORKDIR /app

# Copy the server binary from the builder stage
COPY --from=builder /app/server /app/server

# Expose the port the server listens on
EXPOSE 8080

# Run the server binary
CMD ["/app/server"]
