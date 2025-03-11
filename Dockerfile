FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o purchase-worker ./cmd/purchase-worker

FROM alpine:latest

WORKDIR /purchase-worker

COPY --from=builder /app/purchase-worker .

CMD ["./purchase-worker"]