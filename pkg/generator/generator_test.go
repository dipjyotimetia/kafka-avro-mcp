package generator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerateEmitsBothMCPAdaptersAndFixedToolMetadata(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "order.avsc"), []byte(`{"type":"record","name":"OrderCreated","fields":[{"name":"orderId","type":"string"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "kafka.mcp.yaml"), []byte(`apiVersion: mcp.kafka/v1alpha1
package: events
events:
  - name: order_created
    schema: order.avsc
    kafka: { topic: orders.created, subject: orders.created-value, key: { field: orderId } }
    mcp: { tool: publish_order_created, description: Publish an order. }
`), 0o600); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "gen")
	if err := Generate(filepath.Join(dir, "kafka.mcp.yaml"), out); err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	generated, err := os.ReadFile(filepath.Join(out, "tools.mcp.go"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(generated)
	for _, want := range []string{"RegisterGoSDK", "RegisterMCPGo", "orders.created", "orders.created-value", "publish_order_created"} {
		if !strings.Contains(text, want) {
			t.Errorf("generated output does not contain %q", want)
		}
	}
}
