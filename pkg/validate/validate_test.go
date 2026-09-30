package validate

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"
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

// fakeRegistry answers the two read-only calls RegistryChecker makes and
// records the compatibility path it was asked about.
func fakeRegistry(t *testing.T, registered, compatible bool) (*sr.Client, *string) {
	t.Helper()
	var compatibilityPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/subjects/events-value":
			if !registered {
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(map[string]any{"error_code": 40403, "message": "Schema not found"})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"subject": "events-value", "version": 1, "id": 7, "schema": `{"type":"string"}`})
		case r.Method == http.MethodPost && r.URL.Path == "/compatibility/subjects/events-value/versions/latest":
			compatibilityPath = r.URL.Path
			_ = json.NewEncoder(w).Encode(map[string]any{"is_compatible": compatible})
		default:
			t.Errorf("unexpected registry call %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	client, err := sr.NewClient(sr.URLs(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	return client, &compatibilityPath
}

func TestRegistryCheckerRequiresRegisteredSchemaCompatibleWithLatest(t *testing.T) {
	schema := []byte(`{"type":"record","name":"Event","fields":[{"name":"id","type":"string"}]}`)
	ctx := context.Background()

	client, path := fakeRegistry(t, true, true)
	if err := (RegistryChecker{Client: client}).Check(ctx, "events-value", schema); err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	// Checking against the looked-up version would compare the schema with
	// itself and never fail; the gate must ask about the latest version.
	if *path != "/compatibility/subjects/events-value/versions/latest" {
		t.Errorf("compatibility checked at %q, want the latest version", *path)
	}

	client, _ = fakeRegistry(t, false, true)
	if err := (RegistryChecker{Client: client}).Check(ctx, "events-value", schema); err == nil {
		t.Error("Check() accepted an unregistered schema")
	}

	client, _ = fakeRegistry(t, true, false)
	if err := (RegistryChecker{Client: client}).Check(ctx, "events-value", schema); err == nil {
		t.Error("Check() accepted a schema incompatible with the latest version")
	}

	if err := (RegistryChecker{}).Check(ctx, "events-value", schema); err == nil {
		t.Error("Check() succeeded without a client")
	}
}
