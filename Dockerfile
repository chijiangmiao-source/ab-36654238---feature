# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# build: full toolchain + module cache + source tree
# ---------------------------------------------------------------------------
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server

# ---------------------------------------------------------------------------
# runtime: the service image
# ---------------------------------------------------------------------------
FROM alpine:3.20 AS runtime
RUN apk add --no-cache ca-certificates wget
COPY --from=build /out/server /usr/local/bin/server
ENV PORT=8080
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/server"]

# ---------------------------------------------------------------------------
# verify: one-shot acceptance harness (build checks + unit tests + HTTP smoke)
# ---------------------------------------------------------------------------
FROM build AS verify
RUN CGO_ENABLED=0 go build -trimpath -o /out/verify ./cmd/verify
ENTRYPOINT ["/out/verify"]
