FROM golang:1.25.0-bookworm as builder

WORKDIR /app

# Get dependencies
COPY src/go.mod .
RUN go mod download

# Compile
COPY src/ .
RUN go build -v -o logHandler .

FROM debian:bookworm-slim

ARG PORT=8001
ENV PORT=${PORT}

WORKDIR /app
RUN mkdir -p /app/logs
COPY --from=builder /app/logHandler .

EXPOSE ${PORT}
ENTRYPOINT ["/app/logHandler"]
