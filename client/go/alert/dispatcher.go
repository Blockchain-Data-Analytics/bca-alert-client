// Copyright 2026 Alexander Diemand
// SPDX-License-Identifier: Apache-2.0

package alert

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Dispatcher connects a listener to its sinks. Every sink gets its own
// buffered channel and goroutine, so all sinks receive every message in
// parallel and in order.
//
// When the channel of a sink is full, Dispatch blocks until it has room:
// no alert is lost, but a stuck sink stalls the listener.
type Dispatcher struct {
	// CloseTimeout is how long Close waits for the sinks to finish
	// what is queued; set it before Close.
	CloseTimeout time.Duration

	buf  int
	subs []*subscriber
	wg   sync.WaitGroup
	done chan struct{}
	once sync.Once
}

type subscriber struct {
	name string
	sink Sink
	ch   chan item
}

// item is a message on its way to one sink
type item struct {
	m   Message
	ack *ack
}

// ack counts down the sinks that still have to handle one message
type ack struct {
	left   atomic.Int32
	onDone func()
}

// NewDispatcher: buf is the number of messages queued per sink.
func NewDispatcher(buf int) *Dispatcher {
	return &Dispatcher{buf: buf, CloseTimeout: 10 * time.Second, done: make(chan struct{})}
}

// Add connects a sink and starts its goroutine. Call it before the
// listener starts; name is used in diagnostics.
func (d *Dispatcher) Add(name string, s Sink) {
	sub := &subscriber{name: name, sink: s, ch: make(chan item, d.buf)}
	d.subs = append(d.subs, sub)
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		for {
			select {
			case it := <-sub.ch:
				sub.handle(it)
			case <-d.done:
				// Close: finish what is queued, then stop
				for {
					select {
					case it := <-sub.ch:
						sub.handle(it)
					default:
						return
					}
				}
			}
		}
	}()
}

// Dispatch hands m to every sink. It returns early if ctx is done or the
// dispatcher is closed.
func (d *Dispatcher) Dispatch(ctx context.Context, m Message) {
	d.DispatchAck(ctx, m, nil)
}

// DispatchAck is Dispatch, and calls onDone (if not nil) once every sink has
// finished with m, whether it returned an error or not. onDone calls come
// in the order of the messages, from a sink goroutine. A listener uses it to
// acknowledge a message to its source only after the sinks have it. onDone
// is not called for a message that was not handed to all sinks.
func (d *Dispatcher) DispatchAck(ctx context.Context, m Message, onDone func()) {
	var a *ack
	if onDone != nil && len(d.subs) > 0 {
		a = &ack{onDone: onDone}
		a.left.Store(int32(len(d.subs)))
	}
	for _, sub := range d.subs {
		select {
		case sub.ch <- item{m, a}:
		case <-ctx.Done():
			return
		case <-d.done:
			return
		}
	}
}

// Close lets the sinks process what is queued and waits for them, at most
// CloseTimeout: a stuck sink must not keep the process from exiting.
// Safe to call more than once and while Dispatch is running; messages
// dispatched after Close may be dropped.
func (d *Dispatcher) Close() {
	d.once.Do(func() { close(d.done) })
	drained := make(chan struct{})
	go func() { d.wg.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(d.CloseTimeout):
		Log.Printf("dispatcher: sinks still busy after %s, queued messages are dropped", d.CloseTimeout)
	}
}

// sinks get a context that is never cancelled, so they can finish what is
// queued on shutdown
func (s *subscriber) handle(it item) {
	defer func() {
		if r := recover(); r != nil {
			Log.Printf("sink %s: panic: %v", s.name, r)
		}
		if it.ack != nil && it.ack.left.Add(-1) == 0 {
			it.ack.onDone()
		}
	}()
	if err := s.sink.Handle(context.Background(), it.m); err != nil {
		Log.Printf("sink %s: %v", s.name, err)
	}
}
