# One image, every Slipstream binary. Each compose service picks its binary
# with `command`. The web UI is built in its own stage and served by the api.
#
# The runtime is alpine (not distroless) so compose healthchecks can use wget.
# Binaries are static (CGO disabled).

FROM node:24-alpine AS web
WORKDIR /web
COPY web/ ./
RUN if [ -f package.json ]; then npm ci --no-audit --no-fund && npm run build; else mkdir -p dist; fi

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=docker
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/ ./cmd/...

FROM alpine:3.22
RUN apk add --no-cache wget ca-certificates \
 && adduser -D -u 10001 ss \
 && mkdir -p /data && chown ss /data
COPY --from=build /out/ /usr/local/bin/
COPY --from=web /web/dist /srv/web
USER ss
WORKDIR /data
ENV SS_WEB_DIR=/srv/web
