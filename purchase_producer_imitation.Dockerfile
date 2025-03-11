FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG BUILD_TAGS
RUN CGO_ENABLED=0 GOOS=linux go build -tags "${BUILD_TAGS}" -o purchase-producer-imitation ./cmd/purchase-producer-imitation

FROM scratch

WORKDIR /purchase-producer-imitation

COPY --from=builder /app/internal/testdata ./internal/testdata

COPY --from=builder /app/purchase-producer-imitation .

ENTRYPOINT ["./purchase-producer-imitation"]