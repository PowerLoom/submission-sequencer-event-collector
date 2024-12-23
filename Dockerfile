# Use the official Golang image as the build environment
FROM golang:1.23 AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy go.mod and go.sum files to the working directory
COPY go.mod go.sum ./

# Download the dependencies
RUN go mod download

# Copy the rest of the application code to the working directory
COPY . .

# Ensure the swagger.json file is copied to the correct relative path
RUN mkdir -p /app/pkgs/service/docs
COPY pkgs/service/docs/swagger.json /app/pkgs/service/docs/swagger.json

# Build the Go application
RUN CGO_ENABLED=0 GOOS=linux go build -o /sequencer-event-collector ./cmd/main.go

# Use a minimal base image
FROM scratch

# Copy the binary from the builder stage
COPY --from=builder /sequencer-event-collector /sequencer-event-collector

# Copy the swagger.json file to the same relative path
COPY --from=builder /app/pkgs/service/docs/swagger.json /pkgs/service/docs/swagger.json

# Command to run the application
CMD ["/sequencer-event-collector"]
