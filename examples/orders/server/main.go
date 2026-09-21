// Command server runs the generated orders tools as an MCP server over stdio.
//
// It is the wiring the README describes, in a form you can actually run:
//
//	KAFKA_BROKERS=localhost:9092 \
//	SCHEMA_REGISTRY_URL=http://localhost:8081 \
//	go run ./examples/orders/server
//
// The schema must already be registered under the subject the manifest names;
// the resolver looks schemas up and never registers them, so an unregistered
// or mismatched schema stops the publish rather than creating a new version.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	events "github.com/dipjyotimetia/kafka-avro-mcp/examples/orders/gen"
	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/runtime"
	gomcp "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	if err := run(context.Background()); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	brokers := os.Getenv("KAFKA_BROKERS")
	registryURL := os.Getenv("SCHEMA_REGISTRY_URL")
	if brokers == "" || registryURL == "" {
		return fmt.Errorf("KAFKA_BROKERS and SCHEMA_REGISTRY_URL are required")
	}

	// Stdio is the MCP transport here, so anything written to stdout would
	// corrupt the protocol stream. Logs go to stderr.
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		// Publishing is not idempotent at the tool level — each call is a new
		// record — but the producer should not turn one call into duplicates.
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}
	defer producer.Close()

	registry, err := runtime.NewRegistryClient(registryURL, "")
	if err != nil {
		return fmt.Errorf("schema registry client: %w", err)
	}

	service := runtime.NewService(
		&runtime.RegistryResolver{Client: registry},
		runtime.KafkaPublisher{Client: producer},
		runtime.WithLogger(logger),
	)

	server := gomcp.NewServer(&gomcp.Implementation{Name: "kafka-avro-mcp", Version: "0.1.0"}, nil)
	events.RegisterTools(runtime.WrapGoSDK(server), service)

	logger.Info("serving MCP over stdio", "brokers", brokers, "registry", registryURL)
	return server.Run(ctx, &gomcp.StdioTransport{})
}
