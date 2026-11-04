// Copyright 2026 Alexander Diemand
// SPDX-License-Identifier: Apache-2.0

// Package alert holds what listeners and sinks share: the message type,
// the Sink interface and the Dispatcher connecting them.
package alert

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Envelopes, one per MQTT/Kafka message:
//
//	{"type":"event","payload":{"id":..,"detected_at":..,"slot_no":..,
//	  "epoch_no":..,"block_time":..,"tx_hash":..,"address":..,
//	  "event_type":..,"network":..,"confirmations":..,"data":{..}}}
//	{"type":"heartbeat","last_event_id":..,"channel_status":..,
//	  "network":..,"sent_at":..}

// Message is one decoded delivery. It is shared read-only by all sinks.
type Message struct {
	Channel   string // "mqtt" or "kafka"
	Raw       []byte // payload as received
	Type      string // "event", "heartbeat", other as sent, "" if not JSON
	Event     *Event // set if Type is "event"
	Heartbeat *Heartbeat
	ParseErr  error // Raw is not a valid envelope: a *FieldError if it is JSON
}

// Event is the payload of an "event" envelope. Missing or null fields
// stay at their zero value (nil pointer, empty string).
type Event struct {
	ID            *int64          `json:"id"`
	DetectedAt    string          `json:"detected_at"`
	SlotNo        *int64          `json:"slot_no"`
	EpochNo       *int64          `json:"epoch_no"`
	BlockTime     *string         `json:"block_time"` // null for events from before the field existed
	TxHash        string          `json:"tx_hash"`
	Address       string          `json:"address"`
	EventType     string          `json:"event_type"`
	Network       string          `json:"network"`
	Confirmations *int64          `json:"confirmations"`
	Data          json.RawMessage `json:"data"` // event type specific; kept raw, it may hold integers beyond 64 bits
}

// Heartbeat is sent by the server per network every 10 minutes.
type Heartbeat struct {
	LastEventID   *int64 `json:"last_event_id"`
	ChannelStatus string `json:"channel_status"`
	Network       string `json:"network"`
	SentAt        string `json:"sent_at"`
}

// FieldError is valid JSON with a field of the wrong type, e.g. "slot_no"
// sent as a string where a number is expected.
type FieldError struct {
	Field string // "payload.slot_no"
	Want  string // Go type: int64, string, ...
	Got   string // JSON type: string, number, object, ...
}

func (e *FieldError) Error() string {
	return fmt.Sprintf("field %s: expected %s, got JSON %s", e.Field, e.Want, e.Got)
}

// decodeErr turns the error of json.Unmarshal into a *FieldError where it can
func decodeErr(prefix string, err error) error {
	var te *json.UnmarshalTypeError
	if !errors.As(err, &te) {
		return err
	}
	field := te.Field
	if field == "" {
		field = "(message)"
	}
	if prefix != "" && te.Field != "" {
		field = prefix + "." + field
	}
	return &FieldError{Field: field, Want: te.Type.String(), Got: te.Value}
}

type envelope struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// Parse decodes a received payload. It never fails: a payload that is not
// a valid envelope is returned with ParseErr set and Raw intact.
func Parse(channel string, raw []byte) Message {
	m := Message{Channel: channel, Raw: raw}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		m.ParseErr = decodeErr("", err)
		return m
	}
	m.Type = env.Type
	switch env.Type {
	case "event":
		var ev Event
		if err := json.Unmarshal(env.Payload, &ev); err != nil {
			m.ParseErr = decodeErr("payload", err)
			return m
		}
		m.Event = &ev
	case "heartbeat":
		var hb Heartbeat
		if err := json.Unmarshal(raw, &hb); err != nil {
			m.ParseErr = decodeErr("", err)
			return m
		}
		m.Heartbeat = &hb
	}
	return m
}

// Network is the network of an event or heartbeat, "" if there is none.
func (m Message) Network() string {
	switch {
	case m.Event != nil:
		return m.Event.Network
	case m.Heartbeat != nil:
		return m.Heartbeat.Network
	}
	return ""
}
