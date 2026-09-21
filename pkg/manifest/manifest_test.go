package manifest

import "testing"

func TestLoadRequiresExplicitRegistrySubjectAndValidStringKey(t *testing.T) {
	config, err := Load([]byte(`
apiVersion: mcp.kafka/v1alpha1
package: orders
events:
  - name: order_created
    schema: order-created.avsc
    kafka:
      topic: orders.created
      key:
        field: orderId
      subject: orders.created-value
    mcp:
      tool: publish_order_created
      description: Publish an order-created event.
`))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got := config.Events[0].Kafka.Subject; got != "orders.created-value" {
		t.Fatalf("subject = %q, want explicit configured subject", got)
	}

	_, err = Load([]byte(`
apiVersion: mcp.kafka/v1alpha1
package: orders
events:
  - name: order_created
    schema: order-created.avsc
    kafka:
      topic: orders.created
    mcp:
      tool: publish_order_created
`))
	if err == nil {
		t.Fatal("Load() succeeded without a registry subject")
	}
}

func TestLoadRejectsDuplicateToolNames(t *testing.T) {
	_, err := Load([]byte(`
apiVersion: mcp.kafka/v1alpha1
package: orders
events:
  - name: created
    schema: created.avsc
    kafka: { topic: orders.created, subject: orders.created-value }
    mcp: { tool: publish_order }
  - name: cancelled
    schema: cancelled.avsc
    kafka: { topic: orders.cancelled, subject: orders.cancelled-value }
    mcp: { tool: publish_order }
`))
	if err == nil {
		t.Fatal("Load() succeeded with duplicate MCP tool names")
	}
}

func TestLoadRejectsProviderUnsafeToolName(t *testing.T) {
	_, err := Load([]byte(`
apiVersion: mcp.kafka/v1alpha1
package: orders
events:
  - name: created
    schema: created.avsc
    kafka: { topic: orders.created, subject: orders.created-value }
    mcp: { tool: "publish order" }
`))
	if err == nil {
		t.Fatal("Load() accepted an unsafe MCP tool name")
	}
}

func TestLoadRejectsToolNamesCollidingAfterPascalCasing(t *testing.T) {
	for _, pair := range [][2]string{
		{"publish_order", "publish-order"},
		{"publish_order", "publish__order"},
		{"publish_order", "_publish_order"},
		{"publish_order", "publish_order_"},
	} {
		config := []byte(`
apiVersion: mcp.kafka/v1alpha1
package: orders
events:
  - name: created
    schema: created.avsc
    kafka: { topic: orders.created, subject: orders.created-value }
    mcp: { tool: "` + pair[0] + `" }
  - name: cancelled
    schema: cancelled.avsc
    kafka: { topic: orders.cancelled, subject: orders.cancelled-value }
    mcp: { tool: "` + pair[1] + `" }
`)
		if _, err := Load(config); err == nil {
			t.Errorf("Load() accepted %q and %q, which generate the same identifier %q", pair[0], pair[1], Pascal(pair[0]))
		}
	}
}

func manifestWith(pkg, topic, subject string) []byte {
	return []byte(`
apiVersion: mcp.kafka/v1alpha1
package: ` + pkg + `
events:
  - name: created
    schema: created.avsc
    kafka: { topic: "` + topic + `", subject: "` + subject + `" }
    mcp: { tool: publish_order }
`)
}

func TestLoadRejectsIllegalKafkaTopicNames(t *testing.T) {
	for _, topic := range []string{"orders created", "orders/created", "orders:created", ".", "..", "orders#created"} {
		if _, err := Load(manifestWith("orders", topic, "orders.created-value")); err == nil {
			t.Errorf("Load() accepted %q as a Kafka topic", topic)
		}
	}
	if _, err := Load(manifestWith("orders", "orders.created_v1-a", "orders.created-value")); err != nil {
		t.Errorf("Load() rejected a legal topic: %v", err)
	}
}

func TestLoadRejectsSubjectsUnsafeInARegistryURL(t *testing.T) {
	for _, subject := range []string{"orders created", "orders/created-value", "orders?created", "orders#value"} {
		if _, err := Load(manifestWith("orders", "orders.created", subject)); err == nil {
			t.Errorf("Load() accepted %q as a registry subject", subject)
		}
	}
}

// The package name is interpolated straight into the generated source.
func TestLoadRejectsPackageNamesThatAreNotGoIdentifiers(t *testing.T) {
	for _, pkg := range []string{"my-events", "2events", "my events", "func", "range"} {
		if _, err := Load(manifestWith(pkg, "orders.created", "orders.created-value")); err == nil {
			t.Errorf("Load() accepted %q as a Go package name", pkg)
		}
	}
	if _, err := Load(manifestWith("events", "orders.created", "orders.created-value")); err != nil {
		t.Errorf("Load() rejected a valid package name: %v", err)
	}
}
