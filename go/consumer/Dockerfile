# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS build
WORKDIR /build
COPY go/consumer/go.mod go/consumer/go.sum ./
RUN go mod download
COPY go/consumer/. .
RUN go build -o app .

FROM alpine:3.19
WORKDIR /app
COPY --from=build /build/app .
ENTRYPOINT ["./app"]
