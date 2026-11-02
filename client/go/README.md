# BCA Alerts Go client

## Dependencies

Only Docker. The image contains everything, pinned (see `Dockerfile`):

* Go 1.27.2 (builder image by digest); the runtime is a static binary on distroless
* MQTT: [paho.mqtt.golang](https://github.com/eclipse-paho/paho.mqtt.golang) `v1.5.1`
* Kafka: [franz-go](https://github.com/twmb/franz-go) `v1.22.2` (pure Go, no librdkafka)

## Structure

```
cmd/bca-alert-client/   entry point: selects the listener, connects the sinks
listener/mqtt.go        Listener: MQTT, topic bca/<Customer_id>/events
listener/kafka.go       Listener: Kafka, topic bca.<Customer_id>.events
alert/message.go        Message: the decoded envelope (event or heartbeat)
alert/sink.go           Sink: the interface to implement
alert/dispatcher.go     Dispatcher: delivers every message to all sinks
alert/config.go         environment variables
sink/console.go         Sink: prints the messages to stdout (the demo)
```

A listener connects, subscribes and hands every message to the
`Dispatcher`. The dispatcher gives each connected sink its own buffered
channel and goroutine: **all sinks receive every message**, in parallel,
each in order of arrival. A sink that returns an error or panics is logged
and does not affect the other sinks or the listener.

stdout carries only the sinks' output (the console sink); connection messages and errors go to stderr.

## Writing a sink

Implement `alert.Sink` and connect it in `cmd/bca-alert-client/main.go`
(or in your own `main`, importing the packages):

```go
type Sink interface {
	Handle(ctx context.Context, m alert.Message) error
}

type forward struct{}

func (forward) Handle(_ context.Context, m alert.Message) error {
	if m.Event != nil {
		// m.Event.Address, m.Event.TxHash, m.Event.Data (raw JSON), ...
	}
	return nil // an error is logged, the sink keeps running
}

d := alert.NewDispatcher(256) // messages queued per sink
d.Add("console", sink.NewConsole(os.Stdout))
d.Add("forward", forward{})   // receives the same messages
```

`Handle` is plain synchronous code, called for one message at a time. Do
not modify `m`: it is shared with the other sinks. `alert.FilterNetwork`
wraps a sink to only see one network.

When the queue of a sink is full, the listener waits for it: no alert is
lost, but a stuck sink stalls the listener (and with MQTT, the broker may
drop the connection). Keep `Handle` fast or hand over to a worker.

## Build and test

```sh
docker build -t bca-alert-client-go .
```

The build runs `go vet` and the tests (with the race detector) first; the
image is only produced if they pass. To run only the tests and see their output:

```sh
docker build --target test --progress=plain .
```

The `Dockerfile` pins the builder and runtime images by digest and the Go
modules by `go.mod` / `go.sum`. The tests need no broker.

## Configuration

Credentials are taken from environment variables only. The names and
defaults are those of the [Prolog client](../prolog/README.md#configuration).

| variable | channel | required | default |
|----|----|----|----|
| `Customer_id` | both | yes | |
| `Token` | mqtt | yes | token from [token management](https://www.blockchain-applied.com/cardano-alerts-token-management/) |
| `Kafka_password` | kafka | yes (not with `PLAINTEXT`) | SCRAM password handed out by BCA |
| `Kafka_user` | kafka | no | `cust_<Customer_id>_kafka` |
| `Network` | both | no | all; set `mainnet`, `preprod` or `preview` to only show that network |
| `MQTT_host` | mqtt | no | `mqtt.blockchain-applied.com` |
| `MQTT_port` | mqtt | no | `8883` |
| `MQTT_tls` | mqtt | no | `true` |
| `MQTT_cafile` | mqtt | no | system certificates; a PEM file replaces them |
| `MQTT_client_id` | mqtt | no | random; when set, a persistent session keeps QoS 1 messages while offline |
| `MQTT_watchdog` | mqtt | no | `1500`: seconds without any message before the client exits with status 3; `0` disables it |
| `Kafka_brokers` | kafka | no | `kafka.blockchain-applied.com:9093` (comma separated) |
| `Kafka_group_id` | kafka | no | `bca.<Customer_id>.go-client` (must start with `bca.<Customer_id>.`) |
| `Kafka_offset_reset` | kafka | no | `earliest` (a new group replays the topic's retention); `latest` |
| `Kafka_security_protocol` | kafka | no | `SASL_SSL`; `SASL_PLAINTEXT`/`PLAINTEXT` for a local test broker |

All networks a customer subscribed to arrive on the same topic, see the `network` field.

Exit status: `2` usage or configuration error, `1` connect or subscribe failure, `3` watchdog.

## EXAMPLES

```sh
export Customer_id="12345-67890-..."

# MQTT
export Token=bca_...
docker run --rm -e Token -e Customer_id bca-alert-client-go mqtt

# MQTT, long running: restarted after the watchdog gives up (see Notes)
docker run -d --restart unless-stopped -e Token -e Customer_id bca-alert-client-go mqtt

# Kafka
export Kafka_password=...
docker run --rm -e Kafka_password -e Customer_id bca-alert-client-go kafka
```

`-e Name` without a value passes the variable from your shell, so the
credentials stay out of the command line. Add `-e Network` or any other
variable from the table above the same way. Alternatively put them into a
file `.env` (ignored by git and docker build) and use `--env-file .env`.

output (console sink, same as the Prolog client):

```
2026-09-08T14:55:01.123+00:00 [mqtt/mainnet] event utxo_received  id=423726482049734665 slot=197313019 block_time=2026-09-08T14:55:10Z epoch=654 confirmations=-
    address: addr1q8055...
    tx_hash: 5f2c0a...
    data:    {"lovelace":"1500000","assets":{},"output_index":0}
2026-09-08T15:05:00+00:00 [mqtt/mainnet] heartbeat  last_event_id=423726482049734665 channel_status=ok
```

The client runs until stopped (Ctrl-C, or `docker stop`); queued messages are still delivered to the sinks.

## Notes

* Kafka offsets are committed automatically every 5 s; after stopping the
  client, the last few events may be delivered again on the next start.
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
* `data` is kept as raw JSON, so integers beyond 64 bits (datums) survive.
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
| [paho.mqtt.golang](https://github.com/eclipse-paho/paho.mqtt.golang) | v1.5.1 | `EPL-2.0 OR BSD-3-Clause` | linked into the executable |
| [franz-go](https://github.com/twmb/franz-go) | v1.22.2 | `BSD-3-Clause` | linked into the executable |
| gorilla/websocket, klauspost/compress, pierrec/lz4, franz-go/kmsg, x/net, x/sync | see `go.mod` | `BSD-2-Clause` / `BSD-3-Clause` | linked into the executable |
| Go standard library | 1.27.2 | `BSD-3-Clause` | linked into the executable |
| distroless `static-debian13` | see `Dockerfile` | various | Docker image |

See [NOTICE](NOTICE); the image contains the license texts in
`/usr/share/doc/bca-alert-client/`.
