// Copyright 2026 Alexander Diemand
// SPDX-License-Identifier: Apache-2.0

// Package sink holds the sinks that come with the client.
package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/Blockchain-Data-Analytics/bca-alert-client/client/go/alert"
)

// Console is the demo sink: it writes every message to w, one block of
// lines per event or heartbeat.
type Console struct{ w io.Writer }

func NewConsole(w io.Writer) *Console { return &Console{w: w} }

func (c *Console) Handle(_ context.Context, m alert.Message) error {
	var b strings.Builder
	switch {
	case m.ParseErr != nil:
		fmt.Fprintf(&b, "%s not JSON: %s\n", m.Channel, m.Raw)
	case m.Event != nil:
		ev := m.Event
		fmt.Fprintf(&b, "%s [%s/%s] event %s  id=%s slot=%s block_time=%s epoch=%s confirmations=%s\n",
			dash(ev.DetectedAt), m.Channel, dash(ev.Network), dash(ev.EventType), num(ev.ID),
			num(ev.SlotNo), str(ev.BlockTime), num(ev.EpochNo), num(ev.Confirmations))
		fmt.Fprintf(&b, "    address: %s\n", dash(ev.Address))
		fmt.Fprintf(&b, "    tx_hash: %s\n", dash(ev.TxHash))
		if len(ev.Data) > 0 {
			var data bytes.Buffer
			if json.Compact(&data, ev.Data) != nil {
				data.Reset()
				data.Write(ev.Data)
			}
			fmt.Fprintf(&b, "    data:    %s\n", data.String())
		}
	case m.Heartbeat != nil:
		hb := m.Heartbeat
		fmt.Fprintf(&b, "%s [%s/%s] heartbeat  last_event_id=%s channel_status=%s\n",
			dash(hb.SentAt), m.Channel, dash(hb.Network), num(hb.LastEventID), dash(hb.ChannelStatus))
	default:
		fmt.Fprintf(&b, "%s unknown message: %s\n", m.Channel, m.Raw)
	}
	_, err := io.WriteString(c.w, b.String())
	return err
}

// missing or null fields show as '-'
func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func str(s *string) string {
	if s == nil {
		return "-"
	}
	return dash(*s)
}

func num(n *int64) string {
	if n == nil {
		return "-"
	}
	return fmt.Sprint(*n)
}
