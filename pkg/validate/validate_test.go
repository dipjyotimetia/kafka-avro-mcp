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
