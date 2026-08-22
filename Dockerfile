# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/simulator .

FROM alpine:3.21
RUN addgroup -S simulator && adduser -S -G simulator simulator
COPY --from=build /out/simulator /usr/local/bin/simulator
USER simulator
ENTRYPOINT ["simulator"]
