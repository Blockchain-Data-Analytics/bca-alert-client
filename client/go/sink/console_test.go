// Copyright 2026 Alexander Diemand
// SPDX-License-Identifier: Apache-2.0

package sink

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/Blockchain-Data-Analytics/bca-alert-client/client/go/alert"
)

const event = `{"type":"event","payload":{"id":423726482049734665,"detected_at":"2026-09-08T14:55:01.123+00:00","slot_no":197313019,"epoch_no":654,"block_time":"2026-09-08T14:55:10Z","tx_hash":"5f2c0a","address":"addr1q8055xyz","event_type":"utxo_received","network":"mainnet","confirmations":null,"data":{"lovelace":"1500000","assets":{},"output_index":0}}}`
const heartbeat = `{"type":"heartbeat","last_event_id":423726482049734665,"channel_status":"ok","network":"preprod","sent_at":"2026-09-08T15:05:00+00:00"}`

// same cases as client/prolog/test/test_processor.pl
func TestConsole(t *testing.T) {
	for _, c := range []struct{ name, channel, in, want string }{
		{"event", "mqtt", event,
			"2026-09-08T14:55:01.123+00:00 [mqtt/mainnet] event utxo_received  id=423726482049734665 slot=197313019 block_time=2026-09-08T14:55:10Z epoch=654 confirmations=-\n" +
				"    address: addr1q8055xyz\n    tx_hash: 5f2c0a\n    data:    {\"lovelace\":\"1500000\",\"assets\":{},\"output_index\":0}\n"},
		{"event without block_time", "mqtt",
			`{"type":"event","payload":{"id":1,"detected_at":"2026-09-08T14:55:01Z","slot_no":2,"epoch_no":3,"block_time":null,"tx_hash":"ab","address":"addr1x","event_type":"utxo_spent","network":"mainnet","confirmations":null}}`,
			"2026-09-08T14:55:01Z [mqtt/mainnet] event utxo_spent  id=1 slot=2 block_time=- epoch=3 confirmations=-\n    address: addr1x\n    tx_hash: ab\n"},
		{"heartbeat", "kafka", heartbeat,
			"2026-09-08T15:05:00+00:00 [kafka/preprod] heartbeat  last_event_id=423726482049734665 channel_status=ok\n"},
		{"not json", "mqtt", "not json", "mqtt not JSON: not json\n"},
		{"unknown type", "mqtt", `{"type":"other","x":[true,null]}`, "mqtt unknown message: {\"type\":\"other\",\"x\":[true,null]}\n"},
	} {
		t.Run(c.name, func(t *testing.T) {
			var b bytes.Buffer
			if err := NewConsole(&b).Handle(context.Background(), alert.Parse(c.channel, []byte(c.in))); err != nil {
				t.Fatal(err)
			}
			if b.String() != c.want {
				t.Errorf("got\n%s\nwant\n%s", b.String(), c.want)
			}
		})
	}
}

// a datum integer beyond 64 bits must not lose the event
func TestConsoleBigDatumInteger(t *testing.T) {
	var b bytes.Buffer
	in := `{"type":"event","payload":{"event_type":"rollback","data":{"int":123456789012345678901234567890}}}`
	NewConsole(&b).Handle(context.Background(), alert.Parse("mqtt", []byte(in)))
	if !strings.Contains(b.String(), "event rollback") || !strings.Contains(b.String(), "123456789012345678901234567890") {
		t.Errorf("got %q", b.String())
	}
}
