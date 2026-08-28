FROM golang:1.25.0-alpine AS builder
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/analytics-service ./cmd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/analytics-service /analytics-service
EXPOSE 8084
USER nonroot:nonroot
ENTRYPOINT ["/analytics-service"]
