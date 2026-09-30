package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const exampleConfig = "../../examples/orders/kafka.mcp.yaml"

func TestRunRejectsMisuseAsUsageErrors(t *testing.T) {
	for name, args := range map[string][]string{
		"no subcommand":      nil,
		"unknown subcommand": {"publish", "--config", exampleConfig},
		"missing config":     {"validate"},
		"missing out":        {"generate", "--config", exampleConfig},
		"unknown flag":       {"validate", "--config", exampleConfig, "--verbose"},
		"stray argument":     {"validate", "--config", exampleConfig, "extra"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := run(args, io.Discard, io.Discard); !errors.As(err, new(usageError)) {
				t.Fatalf("run(%q) error = %v, want a usage error", args, err)
			}
		})
	}
}

// A flag that does nothing for the chosen subcommand is reported, not ignored.
func TestRunRejectsFlagsForTheOtherSubcommand(t *testing.T) {
	for name, args := range map[string][]string{
		"registry flag on generate": {"generate", "--config", exampleConfig, "--out", t.TempDir(), "--registry-url", "http://registry"},
		"out on validate":           {"validate", "--config", exampleConfig, "--out", t.TempDir()},
	} {
		t.Run(name, func(t *testing.T) {
			err := run(args, io.Discard, io.Discard)
			if err == nil || errors.As(err, new(usageError)) {
				t.Fatalf("run(%q) error = %v, want a non-usage error", args, err)
			}
		})
	}
}

func TestRunValidatesAndGeneratesTheExample(t *testing.T) {
	// Keep the ambient environment from turning this into a registry call.
	t.Setenv("SCHEMA_REGISTRY_URL", "")
	if err := run([]string{"validate", "--config", exampleConfig}, io.Discard, io.Discard); err != nil {
		t.Fatalf("validate error = %v", err)
	}
	out := t.TempDir()
	if err := run([]string{"generate", "--config", exampleConfig, "--out", out}, io.Discard, io.Discard); err != nil {
		t.Fatalf("generate error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, "tools.mcp.go")); err != nil {
		t.Fatalf("generated file missing: %v", err)
	}
}

func TestRunVersionPrintsBuildMetadata(t *testing.T) {
	var stdout strings.Builder
	if err := run([]string{"version"}, &stdout, io.Discard); err != nil {
		t.Fatalf("run(version) error = %v", err)
	}
	if got, want := stdout.String(), "avro-gen-go-mcp dev (commit none, built unknown)\n"; got != want {
		t.Fatalf("run(version) output = %q, want %q", got, want)
	}
}

func TestRunHelpIsNotAnError(t *testing.T) {
	if err := run([]string{"validate", "-h"}, io.Discard, io.Discard); err != nil {
		t.Fatalf("run(-h) error = %v", err)
	}
}
