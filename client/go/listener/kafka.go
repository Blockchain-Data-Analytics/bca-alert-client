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
//
// A record is committed only after every sink has handled it: the record is
// marked when the dispatcher reports it done, and the marks are committed
// every 5 s and when Run ends. A crash redelivers what was not yet handled.
// Run closes d before it returns, so that the sinks finish their queues
// first; the offsets of those messages are still committed.
func (l *Kafka) Run(ctx context.Context, d *alert.Dispatcher) error {
	topic := "bca." + l.customerID + ".events"
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(l.brokers, ",")...),
		kgo.ClientID("bca-alert-client-go"),
		kgo.ConsumerGroup(l.group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(l.offset),
		// commit only the records marked after the sinks are done with them
		kgo.AutoCommitMarks(),
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
			drain(cl, d)
			return nil
		}
		for _, e := range fetches.Errors() {
			if errors.Is(e.Err, context.Canceled) {
				continue
			}
			alert.Log.Printf("kafka: %s/%d: %v", e.Topic, e.Partition, e.Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			d.DispatchAck(ctx, alert.Parse("kafka", r.Value), func() { cl.MarkCommitRecords(r) })
		})
	}
}

// drain waits for the sinks, then commits the marked records
func drain(cl *kgo.Client, d *alert.Dispatcher) {
	d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.CommitMarkedOffsets(ctx); err != nil {
		alert.Log.Printf("kafka: commit: %v", err)
	}
}
