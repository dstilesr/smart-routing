FROM golang:1.25.0-bookworm as builder

WORKDIR /app

# Get dependencies
COPY src/go.mod .
COPY src/go.sum .
RUN go mod download

# Compile
COPY src/ .
RUN go build -v -o dispatcher .

FROM debian:bookworm-slim

ARG PORT=8080
ENV PORT=${PORT}

WORKDIR /app
COPY --from=builder /app/dispatcher .

EXPOSE ${PORT}
CMD ["./dispatcher"]
