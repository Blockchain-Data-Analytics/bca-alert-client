# BCA Alerts Prolog client

## Dependencies

Only Docker. The image contains everything, pinned (see `Dockerfile`):

* GNU Prolog 1.5.0
* Kafka interface: [pl-kafka](https://github.com/CodiePP/pl_kafka) `v0.1.0`
* MQTT interface: [pl-mqtt](https://github.com/CodiePP/pl_mqtt) `v0.1.0`
* JSON: `pl_toolbox/src/json.pl` of [prolog-utilities](https://github.com/CodiePP/prolog-utilities) `v0.1.0`
* libmosquitto, librdkafka from Debian trixie

## Structure

```
src/bca_alert_client.pl   entry point, selects the receiver from the command line
src/receiver_mqtt.pl      Receiver: MQTT, topic bca/<Customer_id>/events
src/receiver_kafka.pl     Receiver: Kafka, topic bca.<Customer_id>.events
src/processor.pl          Processor: decodes the messages, prints them to stdout
src/config.pl             environment variables, logging to stderr
test/test_processor.pl    tests for the processor (no broker needed)
```

A receiver connects, subscribes and hands every message to
`processor_handle(Channel, Payload, Meta)`. Replace `processor.pl` to feed
the events to a downstream process.

stdout carries only the processor's output; connection messages and errors go to stderr.

## Build and test

```sh
docker build -t bca-alert-client-prolog .
```

The build runs the tests first; the image is only produced if they pass.
To run only the tests and see their output:

```sh
docker build --target test --progress=plain .
```

The `Dockerfile` pins every dependency: base image by digest, Debian
packages by exact version from a fixed snapshot.debian.org date,
GNU Prolog by sha256, pl_mqtt/pl_kafka/prolog-utilities by tag and sha256.

## Configuration

Credentials are taken from environment variables only.

| variable | channel | required | default |
|----|----|----|----|
| `Customer_id` | both | yes | |
| `Token` | mqtt | yes | token from [token management](https://www.blockchain-applied.com/cardano-alerts-token-management/) |
| `Kafka_password` | kafka | yes | SCRAM password handed out by BCA |
| `Kafka_user` | kafka | no | `cust_<Customer_id>_kafka` |
| `Network` | both | no | all; set `mainnet`, `preprod` or `preview` to only show that network |
| `MQTT_host` | mqtt | no | `mqtt.blockchain-applied.com` |
| `MQTT_port` | mqtt | no | `8883` |
| `MQTT_tls` | mqtt | no | `true` |
| `MQTT_cafile` | mqtt | no | first of the usual system CA bundles found |
| `MQTT_client_id` | mqtt | no | random; when set, a persistent session keeps QoS 1 messages while offline |
| `MQTT_watchdog` | mqtt | no | `1500`: seconds without any message before the client exits with status 3; `0` disables it |
| `Kafka_brokers` | kafka | no | `kafka.blockchain-applied.com:9093` |
| `Kafka_group_id` | kafka | no | `bca.<Customer_id>.prolog-client` (must start with `bca.<Customer_id>.`) |
| `Kafka_offset_reset` | kafka | no | `earliest` (a new group replays the topic's retention); `latest` |
| `Kafka_security_protocol` | kafka | no | `SASL_SSL`; `SASL_PLAINTEXT`/`PLAINTEXT` for a local test broker |

All networks a customer subscribed to arrive on the same topic, see the `network` field.

## EXAMPLES

```sh
export Customer_id="12345-67890-..."

# MQTT
export Token=bca_...
docker run --rm -e Token -e Customer_id bca-alert-client-prolog mqtt

# MQTT, long running: restarted after the watchdog gives up (see Notes)
docker run -d --restart unless-stopped -e Token -e Customer_id bca-alert-client-prolog mqtt

# Kafka
export Kafka_password=...
docker run --rm -e Kafka_password -e Customer_id bca-alert-client-prolog kafka
```

`-e Name` without a value passes the variable from your shell, so the
credentials stay out of the command line. Add `-e Network` or any other
variable from the table above the same way. Alternatively put them into a
file `.env` (ignored by git and docker build) and use `--env-file .env`.

output:

```
2026-09-08T14:55:01.123+00:00 [mqtt/mainnet] event utxo_received  id=423726482049734665 slot=197313019 epoch=654 confirmations=-
    address: addr1q8055...
    tx_hash: 5f2c0a...
    data:    {"lovelace":"1500000","assets":{},"output_index":0}
2026-09-08T15:05:00+00:00 [mqtt/mainnet] heartbeat  last_event_id=423726482049734665 channel_status=ok
```

The client runs until stopped (Ctrl-C, or `docker stop`).

## Notes

* Kafka offsets are committed automatically every 5 s; after stopping the
  client, the last few events may be delivered again on the next start.
  A stopped consumer stays in the group until the broker's session timeout
  (45 s); a restart within that time waits for it.
* The MQTT broker authenticates by token (username); the password is unused.
* The MQTT client reconnects on its own after a lost connection and logs
  `mqtt: connection lost` / `mqtt: connection restored` to stderr. The
  server sends a heartbeat per network every 10 minutes, so as a last
  resort a watchdog exits the client with status 3 when no message at all
  arrived for `MQTT_watchdog` seconds (default 25 minutes). Run it under a
  supervisor that restarts it, e.g. `docker run --restart unless-stopped`
  (which cannot be combined with `--rm`). Events published while the
  client was down are not replayed over MQTT (unless `MQTT_client_id` keeps
  a persistent session); fetch them from the Cardano Alerts API with the
  `last_event_id` of the last heartbeat seen.
* Integers too large for GNU Prolog (e.g. in datums) are shown as strings
  in `data`.
* This code has been produced with the help of AI, and reviewed and tested
  by the author.

## Disclaimer

This is a demo client showing how to receive Cardano Alerts; it is not
meant for production use as is. It is provided "AS IS", without warranties
or conditions of any kind (see sections 7 and 8 of the [license](LICENSE)).

Events can be missed or delivered more than once, e.g. while the client is
not running or reconnecting. Do not rely on this client alone for
decisions involving funds; reconcile against the Cardano Alerts API or the
blockchain itself. The output is information, not financial advice.

## License

Copyright 2026 Alexander Diemand

The client source is licensed under the [Apache License, Version 2.0](LICENSE)
(SPDX-License-Identifier: `Apache-2.0`).

Dependencies and their licenses:

| component | version | license (SPDX) | use |
|----|----|----|----|
| [pl_mqtt](https://github.com/CodiePP/pl_mqtt) | v0.1.0 | `GPL-3.0-or-later` | linked into the executable |
| [pl_kafka](https://github.com/CodiePP/pl_kafka) | v0.1.0 | `GPL-3.0-or-later` | linked into the executable |
| [prolog-utilities](https://github.com/CodiePP/prolog-utilities) (`pl_toolbox/src/json.pl`) | v0.1.0 | `GPL-3.0-or-later` | linked into the executable |
| [GNU Prolog](https://www.gprolog.org/) runtime | 1.5.0 | `LGPL-3.0-or-later OR GPL-2.0-or-later` | linked into the executable |
| libmosquitto (Debian `libmosquitto1`) | 2.0.21 | `EPL-2.0 OR BSD-3-Clause` | shared library |
| librdkafka (Debian `librdkafka1`) | 2.8.0 | `BSD-2-Clause` | shared library |
| tini | 0.19.0 | `MIT` | Docker image entrypoint |
| ca-certificates | 20250419 | `MPL-2.0` (certificate data) | Docker image |

OpenSSL, glibc, libcurl, cyrus-sasl and the other libraries librdkafka
depends on come from Debian trixie under their own licenses.

Because the GPL-3.0-or-later libraries are linked in, the executable and the
Docker image are distributed under the terms of the GPL, version 3 or later;
the Apache-2.0 client source is compatible with that. See [NOTICE](NOTICE);
the image contains the license texts in `/usr/share/doc/bca-alert-client/`.
