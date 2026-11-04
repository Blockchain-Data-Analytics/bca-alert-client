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
	var fe *FieldError
	if errors.As(Parse("mqtt", []byte("nope")).ParseErr, &fe) {
		t.Errorf("not JSON must not be a FieldError")
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

// a stuck sink must not keep Close from returning
func TestDispatcherCloseTimeout(t *testing.T) {
	d := NewDispatcher(1)
	d.CloseTimeout = 100 * time.Millisecond
	release := make(chan struct{})
	defer close(release)
	d.Add("stuck", SinkFunc(func(context.Context, Message) error { <-release; return nil }))
	d.Dispatch(context.Background(), Message{})
	returned := make(chan struct{})
	go func() { d.Close(); close(returned) }()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("Close hangs on a stuck sink")
	}
}

// Dispatch after Close, and while Close runs, must not panic and must not block
func TestDispatcherDispatchAfterClose(t *testing.T) {
	d := NewDispatcher(1)
	d.CloseTimeout = 100 * time.Millisecond
	release := make(chan struct{})
	defer close(release)
	d.Add("stuck", SinkFunc(func(context.Context, Message) error { <-release; return nil }))
	d.Dispatch(context.Background(), Message{})
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); d.Dispatch(context.Background(), Message{}) }() // blocked: queue full
	}
	d.Close()
	wg.Wait() // returns because the dispatcher is closed
	d.Dispatch(context.Background(), Message{})
}

// onDone comes once per message, in order, after every sink handled it,
// also if a sink fails or panics
func TestDispatchAck(t *testing.T) {
	d := NewDispatcher(8)
	var mu sync.Mutex
	handled := map[string]int{}
	count := func(name string, delay int, err bool) Sink {
		return SinkFunc(func(_ context.Context, m Message) error {
			mu.Lock()
			handled[name+string(m.Raw)]++
			mu.Unlock()
			time.Sleep(time.Duration(delay) * time.Millisecond) // sinks of different speed
			if err {
				return errors.New("boom")
			}
			return nil
		})
	}
	d.Add("a", count("a", 0, false))
	d.Add("fails", count("fails", 1, true))
	d.Add("panics", SinkFunc(func(context.Context, Message) error { panic("boom") }))
	d.Add("slow", count("slow", 3, false))
	var order []int
	var omu sync.Mutex
	const n = 20
	for i := 0; i < n; i++ {
		i := i
		d.DispatchAck(context.Background(), Message{Raw: []byte{byte('A' + i)}}, func() {
			mu.Lock()
			for _, name := range []string{"a", "fails", "slow"} {
				if handled[name+string(rune('A'+i))] != 1 {
					t.Errorf("onDone(%d) before sink %s handled it", i, name)
				}
			}
			mu.Unlock()
			omu.Lock()
			order = append(order, i)
			omu.Unlock()
		})
	}
	d.Close()
	omu.Lock()
	defer omu.Unlock()
	if len(order) != n {
		t.Fatalf("onDone called %d times, want %d", len(order), n)
	}
	for i, v := range order {
		if v != i {
			t.Fatalf("onDone out of order: %v", order)
		}
	}
}

// valid JSON with a field of the wrong type names the field and both types
func TestParseFieldError(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{`{"type":"event","payload":{"slot_no":"197"}}`, "field payload.slot_no: expected int64, got JSON string"},
		{`{"type":"event","payload":{"network":5}}`, "field payload.network: expected string, got JSON number"},
		{`{"type":"heartbeat","last_event_id":"x"}`, "field last_event_id: expected int64, got JSON string"},
		{`{"type":7}`, "field type: expected string, got JSON number"},
		{`[1]`, "field (message): expected alert.envelope, got JSON array"},
	} {
		m := Parse("mqtt", []byte(c.in))
		var fe *FieldError
		if !errors.As(m.ParseErr, &fe) || fe.Error() != c.want {
			t.Errorf("%s: got %v, want %s", c.in, m.ParseErr, c.want)
		}
	}
}

func TestEnvNetwork(t *testing.T) {
	for in, ok := range map[string]bool{"": true, "mainnet": true, "preprod": true, "preview": false, "mainet": false, "Mainnet": false} {
		t.Setenv("Network", in)
		n, err := EnvNetwork()
		var ce *ConfigError
		if ok && (err != nil || n != in) || !ok && !errors.As(err, &ce) {
			t.Errorf("Network=%q: %q, %v", in, n, err)
		}
	}
}
