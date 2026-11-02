// Copyright 2026 Alexander Diemand
// SPDX-License-Identifier: Apache-2.0

package alert

import (
	"context"
	"sync"
)

// Dispatcher connects a listener to its sinks. Every sink gets its own
// buffered channel and goroutine, so all sinks receive every message in
// parallel and in order.
//
// When the channel of a sink is full, Dispatch blocks until it has room:
// no alert is lost, but a stuck sink stalls the listener.
type Dispatcher struct {
	buf  int
	subs []*subscriber
	wg   sync.WaitGroup
	once sync.Once
}

type subscriber struct {
	name string
	sink Sink
	ch   chan Message
}

// NewDispatcher: buf is the number of messages queued per sink.
func NewDispatcher(buf int) *Dispatcher {
	return &Dispatcher{buf: buf}
}

// Add connects a sink and starts its goroutine. Call it before the
// listener starts; name is used in diagnostics.
func (d *Dispatcher) Add(name string, s Sink) {
	sub := &subscriber{name: name, sink: s, ch: make(chan Message, d.buf)}
	d.subs = append(d.subs, sub)
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		for m := range sub.ch {
			sub.handle(m)
		}
	}()
}

// Dispatch hands m to every sink. It returns early if ctx is done.
func (d *Dispatcher) Dispatch(ctx context.Context, m Message) {
	for _, sub := range d.subs {
		select {
		case sub.ch <- m:
		case <-ctx.Done():
			return
		}
	}
}

// Close lets the sinks process what is queued and waits for them. Call it
// once the listener has returned; Dispatch must not be called afterwards.
func (d *Dispatcher) Close() {
	d.once.Do(func() {
		for _, sub := range d.subs {
			close(sub.ch)
		}
		d.wg.Wait()
	})
}

// sinks get a context that is never cancelled, so they can finish what is
// queued on shutdown
func (s *subscriber) handle(m Message) {
	defer func() {
		if r := recover(); r != nil {
			Log.Printf("sink %s: panic: %v", s.name, r)
		}
	}()
	if err := s.sink.Handle(context.Background(), m); err != nil {
		Log.Printf("sink %s: %v", s.name, err)
	}
}
