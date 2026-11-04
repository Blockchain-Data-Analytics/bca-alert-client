// Copyright 2026 Alexander Diemand
// SPDX-License-Identifier: Apache-2.0

// Command bca-alert-client receives Cardano Alerts over MQTT or Kafka and
// writes them to stdout. To process them differently, implement
// alert.Sink and connect it below with d.Add.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Blockchain-Data-Analytics/bca-alert-client/client/go/alert"
	"github.com/Blockchain-Data-Analytics/bca-alert-client/client/go/listener"
	"github.com/Blockchain-Data-Analytics/bca-alert-client/client/go/sink"
)

// listening is what MQTT and Kafka listeners have in common
type listening interface {
	Run(ctx context.Context, d *alert.Dispatcher) error
}

func main() { os.Exit(run()) }

// exit status: 2 usage or configuration, 1 connect or subscribe failure, 3 watchdog
func run() int {
	if len(os.Args) != 2 || (os.Args[1] != "mqtt" && os.Args[1] != "kafka") {
		fmt.Fprintln(os.Stderr, "usage: bca-alert-client mqtt|kafka")
		return 2
	}

	var l listening
	var err error
	if os.Args[1] == "mqtt" {
		l, err = listener.NewMQTT()
	} else {
		l, err = listener.NewKafka()
	}
	var network string
	if err == nil {
		network, err = alert.EnvNetwork()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// the sinks: every message goes to all of them
	d := alert.NewDispatcher(256)
	var console alert.Sink = sink.NewConsole(os.Stdout)
	if network != "" {
		console = alert.FilterNetwork(network, console)
	}
	d.Add("console", console)

	err = l.Run(ctx, d)
	d.Close()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
	}
	return exitCode(err)
}

// exitCode: 0 clean stop, 3 watchdog, 2 configuration, 1 anything else
func exitCode(err error) int {
	var ce *alert.ConfigError
	switch {
	case err == nil:
		return 0
	case errors.Is(err, listener.ErrWatchdog):
		return 3
	case errors.As(err, &ce):
		return 2
	}
	return 1
}
