# Copyright 2026 Alexander Diemand
# SPDX-License-Identifier: Apache-2.0
#
# BCA Alerts Prolog client (GNU Prolog), built with all dependencies pinned:
#   base image by digest, Debian packages by exact version from a fixed
#   snapshot.debian.org date, GNU Prolog by version + sha256, pl_mqtt,
#   pl_kafka and prolog-utilities by tag + sha256.
#
# build (from this directory; runs the tests first):
#   docker build -t bca-alert-client-prolog .
#
# tests only:
#   docker build --target test --progress=plain .
#
# run:
#   docker run --rm -e Token -e Customer_id bca-alert-client-prolog mqtt
#   docker run --rm -e Kafka_password -e Customer_id bca-alert-client-prolog kafka
#
# to update: bump the base image tag/digest and SNAPSHOT together, then
# re-resolve the package versions (apt-cache policy <pkg> at that snapshot).

ARG BASE_IMAGE=debian:trixie-20261005-slim@sha256:a29215f6a35e51e22adffa17f89e9d2ef06214e64a2bad10d765c46aea49f11f
ARG SNAPSHOT=20261005T000000Z

# --- deps stage: toolchain and libraries -----------------------------------

FROM ${BASE_IMAGE} AS deps
ARG SNAPSHOT

# package versions only stay installable when taken from the snapshot;
# apt still checks the archive signatures, Valid-Until is expected to be past
RUN sed -i \
      -e "s#http://deb.debian.org/debian-security#http://snapshot.debian.org/archive/debian-security/${SNAPSHOT}#" \
      -e "s#http://deb.debian.org/debian#http://snapshot.debian.org/archive/debian/${SNAPSHOT}#" \
      /etc/apt/sources.list.d/debian.sources && \
    apt-get -o Acquire::Check-Valid-Until=false update && \
    apt-get install -y --no-install-recommends \
      gcc=4:14.2.0-1 \
      make=4.4.1-2 \
      libc6-dev=2.41-12+deb13u4 \
      pkg-config=1.8.1-4 \
      curl=8.14.1-2+deb13u5 \
      ca-certificates=20250419 \
      libmosquitto-dev=2.0.21-1 \
      librdkafka-dev=2.8.0-1 \
    && rm -rf /var/lib/apt/lists/*

# GNU Prolog compiled from source
ARG GPROLOG_VERSION=1.5.0
ARG GPROLOG_SHA256=670642b43c0faa27ebd68961efb17ebe707688f91b6809566ddd606139512c01
RUN curl -fsSL -o /tmp/gprolog.tar.gz \
      https://mirrors.kernel.org/gnu/gprolog/gprolog-${GPROLOG_VERSION}.tar.gz && \
    echo "${GPROLOG_SHA256}  /tmp/gprolog.tar.gz" | sha256sum -c - && \
    tar xzf /tmp/gprolog.tar.gz -C /tmp && \
    cd /tmp/gprolog-${GPROLOG_VERSION}/src && \
    ./configure --prefix=/usr/local \
      --without-doc-dir --without-html-dir --without-examples-dir && \
    make && \
    make install && \
    mkdir -p /licenses/gprolog && \
    cp /tmp/gprolog-${GPROLOG_VERSION}/COPYING /licenses/gprolog/ && \
    cd / && rm -rf /tmp/gprolog*

# Prolog libraries by release tag; the sha256 of each tag's archive makes
# a moved tag fail the build instead of changing the code
ARG PL_MQTT_TAG=v0.1.0
ARG PL_MQTT_SHA256=ba3d0d3e64b9b27167d7cb604c95b22deea2c1d341f85ab3423f2f3cec62a5c2
ARG PL_KAFKA_TAG=v0.1.0
ARG PL_KAFKA_SHA256=7a127346682b9ff85f0324f6bb3d5bd16be06c00141af8a4fdacd65f2f0491ba
ARG PL_UTILS_TAG=v0.1.0
ARG PL_UTILS_SHA256=9da5cf31584bba1cbf63f6f5d776945f915293aebcc7eee871ad0aaea45b0fe3
RUN fetch() { \
      curl -fsSL -o /tmp/$1.tar.gz https://github.com/CodiePP/$1/archive/refs/tags/$2.tar.gz && \
      echo "$3  /tmp/$1.tar.gz" | sha256sum -c - && \
      mkdir -p /deps/$1 && \
      tar xzf /tmp/$1.tar.gz -C /deps/$1 --strip-components=1 && \
      rm /tmp/$1.tar.gz && \
      mkdir -p /licenses/$1 && cp /deps/$1/LICENSE /licenses/$1/; \
    } && \
    fetch pl_mqtt ${PL_MQTT_TAG} ${PL_MQTT_SHA256} && \
    fetch pl_kafka ${PL_KAFKA_TAG} ${PL_KAFKA_SHA256} && \
    fetch prolog-utilities ${PL_UTILS_TAG} ${PL_UTILS_SHA256}

# only the JSON part of pl_toolbox: GNU Prolog has a single namespace
ENV JSON_SRCS="/deps/prolog-utilities/pl_toolbox/src/json.pl"

# --- test stage: processor tests, no broker needed -------------------------

FROM deps AS test
WORKDIR /build
COPY src ./src
COPY test ./test
RUN gplc --no-top-level -o test_processor \
      test/test_processor.pl src/config.pl src/processor.pl ${JSON_SRCS} && \
    ./test_processor

# --- build stage: based on test, so the client is only built if tests pass --

FROM test AS build
# the GNU Prolog parts of pl_mqtt and pl_kafka, compiled from source
# (their configure scripts require SWI-Prolog, which is not needed here)
RUN gplc --no-top-level -o bca-alert-client \
      src/bca_alert_client.pl src/config.pl src/processor.pl \
      src/receiver_mqtt.pl src/receiver_kafka.pl \
      ${JSON_SRCS} \
      /deps/pl_mqtt/src/gp-mqtt.pl /deps/pl_mqtt/src/mqtt.pl \
      /deps/pl_mqtt/src/gp-mqtt-c.c /deps/pl_mqtt/src/mqtt_core.c \
      /deps/pl_kafka/src/gp-kafka.pl /deps/pl_kafka/src/kafka.pl \
      /deps/pl_kafka/src/gp-kafka-c.c /deps/pl_kafka/src/kafka_core.c \
      $(for f in $(pkg-config --cflags libmosquitto rdkafka); do printf -- '-C %s ' "$f"; done) \
      $(for f in $(pkg-config --libs libmosquitto rdkafka) -lpthread; do printf -- '-L %s ' "$f"; done)

# --- runtime stage ---------------------------------------------------------

FROM ${BASE_IMAGE}
ARG SNAPSHOT

# the client source is Apache-2.0; linked with the GPL-3.0-or-later
# Prolog libraries, the executable is distributed under GPL-3.0-or-later
# terms (see NOTICE)
LABEL org.opencontainers.image.title="bca-alert-client-prolog" \
      org.opencontainers.image.description="BCA Alerts client (GNU Prolog): receives Cardano Alerts over MQTT or Kafka" \
      org.opencontainers.image.authors="Alexander Diemand" \
      org.opencontainers.image.licenses="Apache-2.0 AND GPL-3.0-or-later"

RUN sed -i \
      -e "s#http://deb.debian.org/debian-security#http://snapshot.debian.org/archive/debian-security/${SNAPSHOT}#" \
      -e "s#http://deb.debian.org/debian#http://snapshot.debian.org/archive/debian/${SNAPSHOT}#" \
      /etc/apt/sources.list.d/debian.sources && \
    apt-get -o Acquire::Check-Valid-Until=false update && \
    apt-get install -y --no-install-recommends \
      ca-certificates=20250419 \
      libmosquitto1=2.0.21-1 \
      librdkafka1=2.8.0-1 \
      tini=0.19.0-3+b8 \
    && rm -rf /var/lib/apt/lists/* && \
    useradd --system --no-create-home bca

COPY --from=build /build/bca-alert-client /usr/local/bin/bca-alert-client
COPY LICENSE NOTICE /usr/share/doc/bca-alert-client/
COPY --from=deps /licenses/ /usr/share/doc/bca-alert-client/third-party/

USER bca
# tini forwards SIGINT/SIGTERM, so Ctrl-C and docker stop work
ENTRYPOINT ["tini", "--", "bca-alert-client"]
CMD ["mqtt"]
