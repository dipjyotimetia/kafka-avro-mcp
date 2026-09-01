package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/generator"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "generate" {
		fmt.Fprintln(os.Stderr, "usage: avro-gen-go-mcp generate --config kafka.mcp.yaml --out ./gen")
		os.Exit(2)
	}
	flags := flag.NewFlagSet("generate", flag.ExitOnError)
	config := flags.String("config", "", "manifest path")
	out := flags.String("out", "", "output directory")
	_ = flags.Parse(os.Args[2:])
	if *config == "" || *out == "" {
		flags.Usage()
		os.Exit(2)
	}
	if err := generator.Generate(*config, *out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
