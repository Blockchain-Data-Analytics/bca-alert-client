// Copyright 2026 Alexander Diemand
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Blockchain-Data-Analytics/bca-alert-client/client/go/alert"
	"github.com/Blockchain-Data-Analytics/bca-alert-client/client/go/listener"
)

// the exit status is part of the contract with the supervisor
func TestExitCode(t *testing.T) {
	for _, c := range []struct {
		err  error
		want int
	}{
		{nil, 0},
		{listener.ErrWatchdog, 3},
		{fmt.Errorf("wrapped: %w", listener.ErrWatchdog), 3},
		{&alert.ConfigError{Msg: "x"}, 2},
		{errors.New("mqtt: connect: refused"), 1},
	} {
		if got := exitCode(c.err); got != c.want {
			t.Errorf("exitCode(%v) = %d, want %d", c.err, got, c.want)
		}
	}
}
