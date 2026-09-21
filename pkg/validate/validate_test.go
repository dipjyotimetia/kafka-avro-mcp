package validate

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type checkerStub struct{ compatible bool }

func (c checkerStub) Check(context.Context, string, []byte) error {
	if !c.compatible {
		return errors.New("incompatible")
	}
	return nil
}

func TestConfigRequiresRegisteredCompatibleSchemas(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "event.avsc"), []byte(`{"type":"record","name":"Event","fields":[{"name":"id","type":"string"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	config := []byte("apiVersion: mcp.kafka/v1alpha1\npackage: events\nevents:\n- name: event\n  schema: event.avsc\n  kafka: { topic: events, subject: events-value }\n  mcp: { tool: publish_event }\n")
	path := filepath.Join(dir, "kafka.mcp.yaml")
	if err := os.WriteFile(path, config, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Config(context.Background(), path, checkerStub{compatible: true}); err != nil {
		t.Fatalf("Config() error = %v", err)
	}
	if err := Config(context.Background(), path, checkerStub{}); err == nil {
		t.Fatal("Config() accepted incompatible schema")
	}
}

// Config must reject everything Generate rejects, so that a manifest accepted
// by `validate` always generates.
func TestConfigRejectsWhateverGenerationRejects(t *testing.T) {
	for _, tc := range []struct{ name, schema, key string }{
		{
			name:   "missing key field",
			schema: `{"type":"record","name":"Event","fields":[{"name":"id","type":"string"}]}`,
			key:    "customerRef",
		},
		{
			name:   "non-string key field",
			schema: `{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}`,
			key:    "id",
		},
		{
			name:   "logical type",
			schema: `{"type":"record","name":"Event","fields":[{"name":"at","type":{"type":"long","logicalType":"timestamp-millis"}}]}`,
		},
		{
			name:   "non-nullable multi-branch union",
			schema: `{"type":"record","name":"Event","fields":[{"name":"id","type":["string","long"]}]}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "event.avsc"), []byte(tc.schema), 0o600); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "kafka.mcp.yaml")
			config := "apiVersion: mcp.kafka/v1alpha1\npackage: events\nevents:\n- name: event\n  schema: event.avsc\n  kafka: { topic: events, subject: events-value, key: { field: " + tc.key + " } }\n  mcp: { tool: publish_event }\n"
			if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := Config(context.Background(), path, nil); err == nil {
				t.Fatal("Config() accepted a manifest that generation would reject")
			}
		})
	}
}
