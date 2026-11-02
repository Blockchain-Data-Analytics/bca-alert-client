// Copyright 2026 Alexander Diemand
// SPDX-License-Identifier: Apache-2.0

package alert

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	m := Parse("mqtt", []byte(`{"type":"event","payload":{"id":7,"network":"preprod","block_time":null,"data":{"a":1}}}`))
	if m.ParseErr != nil || m.Event == nil || *m.Event.ID != 7 || m.Event.BlockTime != nil || m.Network() != "preprod" || string(m.Event.Data) != `{"a":1}` {
		t.Errorf("event: %+v %+v", m, m.Event)
	}
	m = Parse("mqtt", []byte(`{"type":"heartbeat","network":"mainnet"}`))
	if m.Heartbeat == nil || m.Network() != "mainnet" {
		t.Errorf("heartbeat: %+v", m)
	}
	if m = Parse("mqtt", []byte("nope")); m.ParseErr == nil || string(m.Raw) != "nope" || m.Network() != "" {
		t.Errorf("not json: %+v", m)
	}
	if m = Parse("mqtt", []byte(`{"type":"other"}`)); m.Type != "other" || m.Event != nil || m.Heartbeat != nil {
		t.Errorf("unknown: %+v", m)
	}
}

func TestFilterNetwork(t *testing.T) {
	var got []string
	s := FilterNetwork("preprod", SinkFunc(func(_ context.Context, m Message) error {
		got = append(got, string(m.Raw))
		return nil
	}))
	for _, in := range []string{
		`{"type":"heartbeat","network":"mainnet"}`, // dropped
		`{"type":"heartbeat","network":"preprod"}`,
		`{"type":"event","payload":{"network":"mainnet"}}`, // dropped
		`{"type":"other"}`,                                 // no network: passed on
	} {
		s.Handle(context.Background(), Parse("mqtt", []byte(in)))
	}
	if len(got) != 2 {
		t.Errorf("got %v", got)
	}
}

type recorder struct {
	mu   sync.Mutex
	seen []string
}

func (r *recorder) Handle(_ context.Context, m Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, string(m.Raw))
	return nil
}

// every sink gets every message in order, a failing or panicking sink
// does not disturb the others, and Close drains what is queued
func TestDispatcher(t *testing.T) {
	d := NewDispatcher(4)
	a, b := &recorder{}, &recorder{}
	d.Add("a", a)
	d.Add("fails", SinkFunc(func(context.Context, Message) error { return errors.New("boom") }))
	d.Add("panics", SinkFunc(func(context.Context, Message) error { panic("boom") }))
	d.Add("b", b)
	const n = 100
	for i := 0; i < n; i++ {
		d.Dispatch(context.Background(), Message{Raw: []byte{byte(i)}})
	}
	d.Close()
	d.Close() // idempotent
	for _, r := range []*recorder{a, b} {
		if len(r.seen) != n {
			t.Fatalf("got %d messages, want %d", len(r.seen), n)
		}
		for i, s := range r.seen {
			if s[0] != byte(i) {
				t.Fatalf("message %d out of order", i)
			}
		}
	}
}

// a slow sink does not hold up the other sinks while its queue has room;
// a full queue blocks Dispatch until the context is cancelled
func TestDispatcherBlocksWhenFull(t *testing.T) {
	d := NewDispatcher(1)
	release := make(chan struct{})
	d.Add("stuck", SinkFunc(func(context.Context, Message) error { <-release; return nil }))
	fast := &recorder{}
	d.Add("fast", fast)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 5; i++ { // 1 in the sink + 1 queued, the 3rd blocks
			d.Dispatch(ctx, Message{Raw: []byte{byte(i)}})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Dispatch did not return after ctx was cancelled")
	}
	close(release)
	d.Close()
}
