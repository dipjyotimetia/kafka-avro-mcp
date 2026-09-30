package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/generator"
	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/runtime"
	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/validate"
)

const usage = "usage: avro-gen-go-mcp {generate|validate} --config kafka.mcp.yaml"

// usageError marks a mistake in how the command was invoked, which exits 2
// rather than 1.
type usageError struct{ message string }

func (e usageError) Error() string { return e.message }

func main() {
	if err := run(os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		if errors.As(err, new(usageError)) {
			os.Exit(2)
		}
		os.Exit(1)
	}
}

func run(args []string, stderr io.Writer) error {
	if len(args) < 1 || (args[0] != "generate" && args[0] != "validate") {
		return usageError{usage}
	}
	command := args[0]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	config := flags.String("config", "", "manifest path")
	out := flags.String("out", "", "output directory (generate only)")
	registryURL := flags.String("registry-url", "", "Schema Registry URL, or $SCHEMA_REGISTRY_URL (validate only)")
	registryUser := flags.String("registry-user", "", "Schema Registry basic-auth user, or $SCHEMA_REGISTRY_USER (validate only)")
	if err := flags.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return usageError{err.Error()}
	}
	// Anything left over is a mistake, most often a flag after a positional
	// argument, which the flag package would otherwise silently stop at.
	if flags.NArg() > 0 {
		return usageError{fmt.Sprintf("unexpected arguments: %v", flags.Args())}
	}

	if *config == "" {
		flags.Usage()
		return usageError{"--config is required"}
	}

	// A flag that does nothing for the chosen subcommand is far more likely to
	// be a mistake than an intention, so say so rather than ignoring it.
	switch command {
	case "generate":
		if *registryURL != "" || *registryUser != "" {
			return fmt.Errorf("--registry-url and --registry-user do not apply to %q", command)
		}
		if *out == "" {
			flags.Usage()
			return usageError{"--out is required for generate"}
		}
		return generator.Generate(*config, *out)
	default:
		if *out != "" {
			return fmt.Errorf("--out does not apply to %q", command)
		}
		checker, err := registryChecker(*registryURL, *registryUser)
		if err != nil {
			return err
		}
		return validate.Config(context.Background(), *config, checker)
	}
}

// registryChecker builds the read-only Schema Registry gate, or nil when no
// registry is configured.
func registryChecker(url, user string) (validate.Checker, error) {
	client, err := runtime.NewRegistryClient(url, user)
	if err != nil || client == nil {
		return nil, err
	}
	return validate.RegistryChecker{Client: client}, nil
}
