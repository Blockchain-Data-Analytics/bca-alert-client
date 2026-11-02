// Copyright 2026 Alexander Diemand
// SPDX-License-Identifier: Apache-2.0

package listener

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/Blockchain-Data-Analytics/bca-alert-client/client/go/alert"
)

// Kafka listens on bca.<Customer_id>.events as a member of a consumer group.
type Kafka struct {
	customerID, brokers, user, password, group, protocol string
	offset                                               kgo.Offset
}

// NewKafka reads the settings from the environment (*alert.ConfigError if invalid).
func NewKafka() (*Kafka, error) {
	var l Kafka
	var err error
	if l.customerID, err = alert.EnvRequired("Customer_id"); err != nil {
		return nil, err
	}
	l.protocol = alert.Env("Kafka_security_protocol", "SASL_SSL")
	switch l.protocol {
	case "SASL_SSL", "SASL_PLAINTEXT":
		if l.password, err = alert.EnvRequired("Kafka_password"); err != nil {
			return nil, err
		}
	case "PLAINTEXT":
	default:
		return nil, &alert.ConfigError{Msg: "Kafka_security_protocol must be SASL_SSL, SASL_PLAINTEXT or PLAINTEXT, got " + l.protocol}
	}
	l.brokers = alert.Env("Kafka_brokers", "kafka.blockchain-applied.com:9093")
	l.user = alert.Env("Kafka_user", "cust_"+l.customerID+"_kafka")
	// the broker only admits groups with this prefix
	l.group = alert.Env("Kafka_group_id", "bca."+l.customerID+".go-client")
	switch alert.Env("Kafka_offset_reset", "earliest") {
	case "earliest":
		l.offset = kgo.NewOffset().AtStart()
	case "latest":
		l.offset = kgo.NewOffset().AtEnd()
	default:
		return nil, &alert.ConfigError{Msg: "Kafka_offset_reset must be earliest or latest"}
	}
	return &l, nil
}

// Run blocks until ctx is done (nil) or the broker cannot be reached.
// Offsets are committed automatically every 5 s.
func (l *Kafka) Run(ctx context.Context, d *alert.Dispatcher) error {
	topic := "bca." + l.customerID + ".events"
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(l.brokers, ",")...),
		kgo.ClientID("bca-alert-client-go"),
		kgo.ConsumerGroup(l.group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(l.offset),
	}
	if l.protocol != "PLAINTEXT" {
		opts = append(opts, kgo.SASL(scram.Auth{User: l.user, Pass: l.password}.AsSha512Mechanism()))
	}
	if l.protocol == "SASL_SSL" {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	defer cl.Close()

	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = cl.Ping(pctx)
	cancel()
	if err != nil {
		return fmt.Errorf("kafka: connect %s: %w", l.brokers, err)
	}
	alert.Log.Printf("kafka: connected to %s, listening on %s as group %s", l.brokers, topic, l.group)

	for {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		for _, e := range fetches.Errors() {
			if errors.Is(e.Err, context.Canceled) {
				continue
			}
			alert.Log.Printf("kafka: %s/%d: %v", e.Topic, e.Partition, e.Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			d.Dispatch(ctx, alert.Parse("kafka", r.Value))
		})
	}
}
