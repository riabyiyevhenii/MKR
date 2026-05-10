# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS build
WORKDIR /build
COPY go/producer/go.mod go/producer/go.sum ./
RUN go mod download
COPY go/producer/. .
RUN go build -o app .

FROM alpine:3.19
WORKDIR /app
COPY --from=build /build/app .
ENTRYPOINT ["./app"]
