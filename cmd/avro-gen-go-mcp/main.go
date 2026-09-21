package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/generator"
	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/runtime"
	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/validate"
)

const usage = "usage: avro-gen-go-mcp {generate|validate} --config kafka.mcp.yaml"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 1 || (args[0] != "generate" && args[0] != "validate") {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	command := args[0]
	flags := flag.NewFlagSet(command, flag.ExitOnError)
	config := flags.String("config", "", "manifest path")
	out := flags.String("out", "", "output directory (generate only)")
	registryURL := flags.String("registry-url", "", "Schema Registry URL, or $SCHEMA_REGISTRY_URL (validate only)")
	registryUser := flags.String("registry-user", "", "Schema Registry basic-auth user, or $SCHEMA_REGISTRY_USER (validate only)")
	_ = flags.Parse(args[1:])

	if *config == "" {
		flags.Usage()
		os.Exit(2)
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
			os.Exit(2)
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
