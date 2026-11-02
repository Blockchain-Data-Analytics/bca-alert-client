// Copyright 2026 Alexander Diemand
// SPDX-License-Identifier: Apache-2.0

package listener

import (
	"errors"
	"testing"

	"github.com/Blockchain-Data-Analytics/bca-alert-client/client/go/alert"
)

func isConfigErr(err error) bool {
	var ce *alert.ConfigError
	return errors.As(err, &ce)
}

func TestNewMQTTConfig(t *testing.T) {
	t.Setenv("Customer_id", "")
	t.Setenv("Token", "")
	if _, err := NewMQTT(); !isConfigErr(err) {
		t.Errorf("missing Customer_id: %v", err)
	}
	t.Setenv("Customer_id", "c1")
	if _, err := NewMQTT(); !isConfigErr(err) {
		t.Errorf("missing Token: %v", err)
	}
	t.Setenv("Token", "t")
	l, err := NewMQTT()
	if err != nil || l.port != 8883 || l.watchdog != 1500 || !l.useTLS || l.host != "mqtt.blockchain-applied.com" {
		t.Errorf("defaults: %+v %v", l, err)
	}
	t.Setenv("MQTT_port", "abc")
	if _, err := NewMQTT(); !isConfigErr(err) {
		t.Errorf("bad MQTT_port: %v", err)
	}
	t.Setenv("MQTT_port", "1883")
	t.Setenv("MQTT_cafile", "/nonexistent")
	if _, err := NewMQTT(); !isConfigErr(err) {
		t.Errorf("bad MQTT_cafile: %v", err)
	}
}

func TestNewKafkaConfig(t *testing.T) {
	t.Setenv("Customer_id", "c1")
	t.Setenv("Kafka_password", "")
	if _, err := NewKafka(); !isConfigErr(err) {
		t.Errorf("missing Kafka_password: %v", err)
	}
	t.Setenv("Kafka_password", "p")
	l, err := NewKafka()
	if err != nil || l.user != "cust_c1_kafka" || l.group != "bca.c1.go-client" {
		t.Errorf("defaults: %+v %v", l, err)
	}
	t.Setenv("Kafka_security_protocol", "bogus")
	if _, err := NewKafka(); !isConfigErr(err) {
		t.Errorf("bad protocol: %v", err)
	}
	t.Setenv("Kafka_security_protocol", "PLAINTEXT")
	t.Setenv("Kafka_password", "")
	if _, err := NewKafka(); err != nil {
		t.Errorf("PLAINTEXT needs no password: %v", err)
	}
}
