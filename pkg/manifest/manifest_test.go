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
