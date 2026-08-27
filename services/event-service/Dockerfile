FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/event-service ./cmd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/event-service /event-service
EXPOSE 8082
USER nonroot:nonroot
ENTRYPOINT ["/event-service"]
