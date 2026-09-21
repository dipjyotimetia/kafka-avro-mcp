// Package validate performs local and read-only Schema Registry validation.
package validate

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/jsonschema"
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
		// Mirror the generator's checks so that a manifest accepted here always
		// generates; jsonschema.Convert rejects Avro outside the V1 subset.
		if _, err := jsonschema.Convert(schema); err != nil {
			return fmt.Errorf("convert schema for %s: %w", event.Name, err)
		}
		if err := jsonschema.ValidateKey(event.Kafka.Key.Field, schema); err != nil {
			return fmt.Errorf("validate key for %s: %w", event.Name, err)
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
		return fmt.Errorf("no Schema Registry client configured")
	}
	if _, err := c.Client.LookupSchema(ctx, subject, sr.Schema{Schema: string(schema), Type: sr.TypeAvro}); err != nil {
		return fmt.Errorf("schema is not registered: %w", err)
	}
	// -1 checks against the subject's latest version. Passing the looked-up
	// version would compare the schema against itself and never fail; a pinned
	// producer can legitimately hold an older version that the subject's
	// current compatibility level no longer accepts.
	compatible, err := c.Client.CheckCompatibility(ctx, subject, -1, sr.Schema{Schema: string(schema), Type: sr.TypeAvro})
	if err != nil {
		return err
	}
	if !compatible.Is {
		return fmt.Errorf("schema is incompatible with subject policy")
	}
	return nil
}
