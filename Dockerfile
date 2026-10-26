# syntax=docker/dockerfile:1
# Build go
FROM golang:1.26.8-alpine AS builder
WORKDIR /app
COPY . .
COPY --from=ravel_core / /ravel-go/
ENV CGO_ENABLED=0
ARG VERSION=dev
RUN apk --no-cache add git
RUN go run ./script/prepare-xray
RUN GOEXPERIMENT=jsonv2 go mod download
RUN GOEXPERIMENT=jsonv2 go build -trimpath -ldflags="-s -w -X github.com/YeJianbo/ravel/cmd.version=${VERSION}" -o ravel

# Release
FROM  alpine
# 安装必要的工具包
RUN  apk --update --no-cache add tzdata ca-certificates \
    && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN mkdir -p /etc/ravel
COPY --from=builder /app/ravel /usr/local/bin/ravel

ENV GODEBUG=http2xconnect=1
ENTRYPOINT [ "ravel", "run"]
