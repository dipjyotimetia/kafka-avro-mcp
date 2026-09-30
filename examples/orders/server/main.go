// Command server runs the generated orders tools as an MCP server over stdio.
//
// It is the wiring the README describes, in a form you can actually run:
//
//	KAFKA_BROKERS=localhost:9092 \
//	SCHEMA_REGISTRY_URL=http://localhost:8081 \
//	go run ./examples/orders/server
//
// Set KAFKA_TLS=true to dial brokers over TLS with the system roots, and
// KAFKA_SASL_MECHANISM (PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512) with
// KAFKA_SASL_USER and KAFKA_SASL_PASSWORD to authenticate. The registry is
// reached over whatever scheme SCHEMA_REGISTRY_URL names, with basic auth
// from SCHEMA_REGISTRY_USER and SCHEMA_REGISTRY_PASSWORD.
//
// The schema must already be registered under the subject the manifest names;
// the resolver looks schemas up and never registers them, so an unregistered
// or mismatched schema stops the publish rather than creating a new version.
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	events "github.com/dipjyotimetia/kafka-avro-mcp/examples/orders/gen"
	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/runtime"
	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/runtime/gosdk"
	gomcp "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
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

	tlsConfig, mechanism, err := kafkaSecurity(os.Getenv)
	if err != nil {
		return err
	}
	options := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		// Publishing is not idempotent at the tool level — each call is a new
		// record — but the producer should not turn one call into duplicates.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// The default is to retry an unreachable broker indefinitely; bound it
		// so a failed publish reports back instead of hanging the tool call.
		kgo.RecordDeliveryTimeout(30 * time.Second),
	}
	if tlsConfig != nil {
		options = append(options, kgo.DialTLSConfig(tlsConfig))
	}
	if mechanism != nil {
		options = append(options, kgo.SASL(mechanism))
	}
	producer, err := kgo.NewClient(options...)
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
	events.RegisterTools(gosdk.Wrap(server), service)

	logger.Info("serving MCP over stdio", "brokers", brokers, "registry", registryURL)
	return server.Run(ctx, &gomcp.StdioTransport{})
}

// kafkaSecurity reads the optional broker TLS and SASL settings. Leaving them
// all unset keeps the plaintext, unauthenticated connection a local broker
// expects.
func kafkaSecurity(getenv func(string) string) (*tls.Config, sasl.Mechanism, error) {
	var tlsConfig *tls.Config
	if value := getenv("KAFKA_TLS"); value != "" {
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			return nil, nil, fmt.Errorf("KAFKA_TLS: %w", err)
		}
		if enabled {
			tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
	}

	name := strings.ToUpper(getenv("KAFKA_SASL_MECHANISM"))
	if name == "" {
		return tlsConfig, nil, nil
	}
	user, password := getenv("KAFKA_SASL_USER"), getenv("KAFKA_SASL_PASSWORD")
	if user == "" || password == "" {
		return nil, nil, fmt.Errorf("KAFKA_SASL_MECHANISM %s needs KAFKA_SASL_USER and KAFKA_SASL_PASSWORD", name)
	}
	switch name {
	case "PLAIN":
		return tlsConfig, plain.Auth{User: user, Pass: password}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return tlsConfig, scram.Auth{User: user, Pass: password}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return tlsConfig, scram.Auth{User: user, Pass: password}.AsSha512Mechanism(), nil
	default:
		return nil, nil, fmt.Errorf("KAFKA_SASL_MECHANISM %q is not one of PLAIN, SCRAM-SHA-256, SCRAM-SHA-512", name)
	}
}
