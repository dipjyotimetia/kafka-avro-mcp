package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/generator"
	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/validate"
	"github.com/twmb/franz-go/pkg/sr"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: avro-gen-go-mcp {generate|validate} --config kafka.mcp.yaml")
		os.Exit(2)
	}
	flags := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	config := flags.String("config", "", "manifest path")
	out := flags.String("out", "", "output directory (generate only)")
	registryURL := flags.String("registry-url", "", "Schema Registry URL (validate only)")
	_ = flags.Parse(os.Args[2:])
	if *config == "" || (os.Args[1] == "generate" && *out == "") {
		flags.Usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "generate":
		if err := generator.Generate(*config, *out); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "validate":
		var checker validate.Checker
		if *registryURL != "" {
			client, err := sr.NewClient(sr.URLs(*registryURL))
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			checker = validate.RegistryChecker{Client: client}
		}
		if err := validate.Config(context.Background(), *config, checker); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "usage: avro-gen-go-mcp {generate|validate} --config kafka.mcp.yaml")
		os.Exit(2)
	}
}
