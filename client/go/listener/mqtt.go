// Copyright 2026 Alexander Diemand
// SPDX-License-Identifier: Apache-2.0

// Package listener holds the channels that deliver alerts: MQTT and Kafka.
// A listener connects, subscribes and hands every message to a Dispatcher.
package listener

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/Blockchain-Data-Analytics/bca-alert-client/client/go/alert"
)

// ErrWatchdog is returned by Run when no message arrived for too long;
// run the client under a supervisor that restarts it.
var ErrWatchdog = errors.New("mqtt: watchdog: no message")

// MQTT listens on bca/<Customer_id>/events.
type MQTT struct {
	customerID, token, host, clientID string
	port, watchdog                    int // watchdog in seconds, 0 = off
	useTLS                            bool
	tlsConfig                         *tls.Config
}

// NewMQTT reads the settings from the environment (*alert.ConfigError if invalid).
func NewMQTT() (*MQTT, error) {
	var l MQTT
	var err error
	if l.customerID, err = alert.EnvRequired("Customer_id"); err != nil {
		return nil, err
	}
	if l.token, err = alert.EnvRequired("Token"); err != nil {
		return nil, err
	}
	l.host = alert.Env("MQTT_host", "mqtt.blockchain-applied.com")
	if l.port, err = alert.EnvInt("MQTT_port", 8883); err != nil {
		return nil, err
	}
	if l.watchdog, err = alert.EnvInt("MQTT_watchdog", 1500); err != nil {
		return nil, err
	}
	l.useTLS = alert.EnvBool("MQTT_tls", true)
	l.clientID = alert.Env("MQTT_client_id", "")
	if !l.useTLS && !isLocal(l.host) {
		alert.Log.Printf("warning: MQTT_tls is off: the token is sent unencrypted to %s", l.host)
	}
	l.tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	if f := alert.Env("MQTT_cafile", ""); f != "" {
		pem, err := os.ReadFile(f)
		if err != nil {
			return nil, &alert.ConfigError{Msg: fmt.Sprintf("MQTT_cafile: %v", err)}
		}
		l.tlsConfig.RootCAs = x509.NewCertPool()
		if !l.tlsConfig.RootCAs.AppendCertsFromPEM(pem) {
			return nil, &alert.ConfigError{Msg: "MQTT_cafile: no certificate found in " + f}
		}
	}
	return &l, nil
}

// Run blocks until ctx is done (nil), the connection cannot be set up or
// the watchdog fires (ErrWatchdog). The library reconnects and
// resubscribes by itself after a lost connection.
func (l *MQTT) Run(ctx context.Context, d *alert.Dispatcher) error {
	topic := "bca/" + l.customerID + "/events"
	scheme := "tcp"
	if l.useTLS {
		scheme = "ssl"
	}
	clientID := l.clientID
	if clientID == "" {
		b := make([]byte, 8)
		rand.Read(b)
		clientID = "bca-alert-client-go-" + hex.EncodeToString(b)
	}

	var last atomic.Int64 // unix time of the last message; heartbeats count
	last.Store(time.Now().Unix())
	errc := make(chan error, 1)
	fail := func(err error) {
		select {
		case errc <- err:
		default:
		}
	}

	var connected atomic.Bool // OnConnect runs on library goroutines
	opts := mqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("%s://%s:%d", scheme, l.host, l.port)).
		SetClientID(clientID).
		// the broker authenticates by token only, the password is unused
		SetUsername(l.token).SetPassword("x").
		SetTLSConfig(l.tlsConfig).
		// a fixed client id asks for a persistent session: QoS 1 messages are kept while offline
		SetCleanSession(l.clientID == "").
		SetConnectTimeout(10 * time.Second).
		SetAutoReconnect(true).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			alert.Log.Printf("mqtt: connection lost, waiting for reconnect: %v", err)
		}).
		SetOnConnectHandler(func(c mqtt.Client) {
			if connected.Swap(true) {
				alert.Log.Println("mqtt: connection restored")
			}
			t := c.Subscribe(topic, 1, func(_ mqtt.Client, msg mqtt.Message) {
				last.Store(time.Now().Unix())
				// the payload buffer is only valid during the callback
				d.Dispatch(ctx, alert.Parse("mqtt", append([]byte(nil), msg.Payload()...)))
			})
			if t.Wait() && t.Error() != nil {
				fail(fmt.Errorf("mqtt: subscribe %s: %w", topic, t.Error()))
			}
		})

	client := mqtt.NewClient(opts)
	t := client.Connect()
	if !t.WaitTimeout(15 * time.Second) {
		return errors.New("mqtt: connect: timeout")
	}
	if err := t.Error(); err != nil {
		return fmt.Errorf("mqtt: connect: %w", err)
	}
	defer client.Disconnect(250)
	alert.Log.Printf("mqtt: connected to %s:%d, listening on %s", l.host, l.port, topic)

	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errc:
			return err
		case <-tick.C:
			if quiet, expired := watchdogExpired(time.Unix(last.Load(), 0), time.Now(), l.watchdog); expired {
				alert.Log.Printf("mqtt: watchdog: no message for ~%ds (MQTT_watchdog=%d, connected=%t), exiting",
					int(quiet.Seconds()), l.watchdog, client.IsConnectionOpen())
				return ErrWatchdog
			}
		}
	}
}

// watchdogExpired: no message since last for at least limit seconds;
// a limit of 0 switches the watchdog off
func watchdogExpired(last, now time.Time, limit int) (quiet time.Duration, expired bool) {
	quiet = now.Sub(last)
	return quiet, limit > 0 && quiet >= time.Duration(limit)*time.Second
}

// isLocal: the broker runs on this machine, so nothing crosses the network
func isLocal(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
