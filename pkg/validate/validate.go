// Package validate performs local and read-only Schema Registry validation.
package validate

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/manifest"
	"github.com/twmb/avro"
	"github.com/twmb/franz-go/pkg/sr"
)

type Checker interface {
	Check(context.Context, string, []byte) error
}

func Config(ctx context.Context, configPath string, checker Checker) error {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	config, err := manifest.Load(data)
	if err != nil {
		return err
	}
	for _, event := range config.Events {
		schema, err := os.ReadFile(filepath.Join(filepath.Dir(configPath), event.Schema))
		if err != nil {
			return fmt.Errorf("read schema for %s: %w", event.Name, err)
		}
		if _, err := avro.Parse(string(schema)); err != nil {
			return fmt.Errorf("parse schema for %s: %w", event.Name, err)
		}
		if checker != nil {
			if err := checker.Check(ctx, event.Kafka.Subject, schema); err != nil {
				return fmt.Errorf("validate subject %q: %w", event.Kafka.Subject, err)
			}
		}
	}
	return nil
}

type RegistryChecker struct{ Client *sr.Client }

func (c RegistryChecker) Check(ctx context.Context, subject string, schema []byte) error {
	if c.Client == nil {
		return fmt.Errorf("Schema Registry client is required")
	}
	found, err := c.Client.LookupSchema(ctx, subject, sr.Schema{Schema: string(schema), Type: sr.TypeAvro})
	if err != nil {
		return fmt.Errorf("schema is not registered: %w", err)
	}
	compatible, err := c.Client.CheckCompatibility(ctx, subject, found.Version, sr.Schema{Schema: string(schema), Type: sr.TypeAvro})
	if err != nil {
		return err
	}
	if !compatible.Is {
		return fmt.Errorf("schema is incompatible with subject policy")
	}
	return nil
}
