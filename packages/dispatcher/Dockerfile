FROM golang:1.25.0-bookworm as builder

WORKDIR /app

# Get dependencies
COPY dispatcher/go.mod .
COPY dispatcher/go.sum .
RUN go mod download

# Compile
COPY dispatcher/ .
RUN go build -v -o dispatcher .

FROM debian:bookworm-slim

ARG PORT=8080
ENV PORT=${PORT}

WORKDIR /app
COPY --from=builder /app/dispatcher .

EXPOSE ${PORT}
ENTRYPOINT ["/app/dispatcher"]
