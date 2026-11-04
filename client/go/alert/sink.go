// Copyright 2026 Alexander Diemand
// SPDX-License-Identifier: Apache-2.0

package alert

import (
	"context"
	"log"
	"os"
)

// Sink processes the messages captured by a listener. To process them
// differently, implement Handle and Add the sink to the Dispatcher.
//
// Handle runs in its own goroutine, one message after the other in the
// order of arrival, so it can be plain synchronous code. A returned error
// is logged and does not stop the sink, the other sinks or the listener.
// m is shared with the other sinks: do not modify it.
type Sink interface {
	Handle(ctx context.Context, m Message) error
}

// SinkFunc makes a function a Sink.
type SinkFunc func(ctx context.Context, m Message) error

func (f SinkFunc) Handle(ctx context.Context, m Message) error { return f(ctx, m) }

// Log receives diagnostics. stdout carries only the sinks' output.
var Log = log.New(os.Stderr, "", log.LstdFlags)

// FilterNetwork passes on only the messages of one network (mainnet,
// preprod); all networks share one topic per customer. Messages
// without a network are passed on.
func FilterNetwork(network string, next Sink) Sink {
	return SinkFunc(func(ctx context.Context, m Message) error {
		if n := m.Network(); n != "" && n != network {
			return nil
		}
		return next.Handle(ctx, m)
	})
}
