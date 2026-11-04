// Copyright 2026 Alexander Diemand
// SPDX-License-Identifier: Apache-2.0

package alert

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
)

// ConfigError is a missing or invalid setting; the program exits with 2.
type ConfigError struct{ Msg string }

func (e *ConfigError) Error() string { return e.Msg }

// Env returns the environment variable, or def if it is unset or empty.
func Env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// EnvRequired returns the variable, or a *ConfigError if it is unset or empty.
func EnvRequired(name string) (string, error) {
	if v := os.Getenv(name); v != "" {
		return v, nil
	}
	return "", &ConfigError{fmt.Sprintf("environment variable %s is not set", name)}
}

// EnvInt: the variable must be an integer if set.
func EnvInt(name string, def int) (int, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, &ConfigError{fmt.Sprintf("environment variable %s must be an integer, got '%s'", name, v)}
	}
	return n, nil
}

// EnvBool: 1/true/yes/on are true, anything else is false.
func EnvBool(name string, def bool) bool {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// KnownNetworks are the networks BCA runs brokers for.
var KnownNetworks = []string{"mainnet", "preprod"}

// EnvNetwork returns the optional Network filter: "" for all networks,
// otherwise one of KnownNetworks. Anything else is a *ConfigError: a
// mistyped name would drop every message without a hint.
func EnvNetwork() (string, error) {
	n := os.Getenv("Network")
	if n == "" || slices.Contains(KnownNetworks, n) {
		return n, nil
	}
	return "", &ConfigError{fmt.Sprintf("environment variable Network must be one of %v, got '%s'", KnownNetworks, n)}
}
