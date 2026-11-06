# Copyright 2026 Alexander Diemand
# SPDX-License-Identifier: Apache-2.0
#
# BCA Alerts Go client, built with all dependencies pinned:
#   builder and runtime images by digest, Go modules by go.mod/go.sum
#   (go.sum holds the hash of every module).
#
# build (from this directory; runs the tests first):
#   docker build -t bca-alert-client-go .
#
# tests only:
#   docker build --target test --progress=plain .
#
# run:
#   docker run --rm -e Token -e Customer_id bca-alert-client-go mqtt
#   docker run --rm -e Kafka_password -e Customer_id bca-alert-client-go kafka
#
# to update: bump GO_IMAGE and RUNTIME_IMAGE tag and digest together
# (docker buildx imagetools inspect <image>), modules with go get + go mod tidy.

ARG GO_IMAGE=golang:1.27.2@sha256:820153096fda4de6ac1b7194d12e416b7dd8f4e9a33d17e0345b111b945162d8
ARG RUNTIME_IMAGE=gcr.io/distroless/static-debian13:nonroot@sha256:25ac8f0c3b2334a9eb05452266bd4697864a8c8ef0ef71e5e6480d7f92868329

# --- deps stage: modules ---------------------------------------------------

# runs natively on the build host; Go cross-compiles for the target (no emulation)
FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS deps
# never fetch another toolchain than the one of the image
ENV GOTOOLCHAIN=local GOFLAGS=-mod=readonly
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# --- test stage: vet and tests, no broker needed ---------------------------

FROM deps AS test
COPY alert ./alert
COPY listener ./listener
COPY sink ./sink
COPY cmd ./cmd
RUN test -z "$(gofmt -l .)" && go vet ./... && go test -race ./...

# --- build stage: based on test, so the client is only built if tests pass -

FROM test AS build
ARG TARGETOS TARGETARCH
ENV CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH
RUN go build -trimpath -ldflags="-s -w" -o /out/bca-alert-client ./cmd/bca-alert-client && \
    go list -deps -f '{{with .Module}}{{if not .Main}}{{.Path}} {{.Dir}}{{end}}{{end}}' ./cmd/bca-alert-client | sort -u | \
    while read -r path dir; do \
      mkdir -p "/licenses/$path" && cp "$dir"/LICENSE* "$dir"/NOTICE* "/licenses/$path/" 2>/dev/null || true; \
    done

# --- runtime stage ---------------------------------------------------------

FROM ${RUNTIME_IMAGE}

LABEL org.opencontainers.image.title="bca-alert-client-go" \
      org.opencontainers.image.description="BCA Alerts client (Go): receives Cardano Alerts over MQTT or Kafka" \
      org.opencontainers.image.authors="Alexander Diemand" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=build /out/bca-alert-client /usr/local/bin/bca-alert-client
COPY LICENSE NOTICE /usr/share/doc/bca-alert-client/
COPY --from=build /licenses/ /usr/share/doc/bca-alert-client/third-party/

# static binary, no libraries; it stops on SIGINT/SIGTERM itself
USER nonroot
ENTRYPOINT ["/usr/local/bin/bca-alert-client"]
CMD ["mqtt"]
