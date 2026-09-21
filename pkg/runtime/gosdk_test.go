package runtime

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/jsonschema"
	gomcp "github.com/modelcontextprotocol/go-sdk/mcp"
)

// connectGoSDK registers a tool on a real go-sdk server and returns a client
// session talking to it over an in-memory transport. The go-sdk path had no
// test at all, which is how it went unnoticed that (*Server).AddTool — the
// method this adapter uses — performs no input validation of its own.
func connectGoSDK(t *testing.T, avroSchema string) (*gomcp.ClientSession, *publisherStub) {
	t.Helper()
	input, err := jsonschema.Convert([]byte(avroSchema))
	if err != nil {
		t.Fatalf("Convert() error = %v", err)
	}
	publisher := &publisherStub{}
	server := gomcp.NewServer(&gomcp.Implementation{Name: "kafka-avro-mcp", Version: "0.0.1"}, nil)
	RegisterTool(WrapGoSDK(server), NewService(resolverStub{id: 9}, publisher),
		Tool{Name: "publish_order", Topic: "orders.created", Subject: "orders.created-value", KeyField: "id", Schema: []byte(avroSchema)},
		json.RawMessage(input), "Publish an order.")

	ctx := context.Background()
	serverTransport, clientTransport := gomcp.NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = serverSession.Close() })
	session, err := gomcp.NewClient(&gomcp.Implementation{Name: "test", Version: "0.0.1"}, nil).Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session, publisher
}

const orderSchema = `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"amount","type":"double"}]}`

func TestWrapGoSDKAdvertisesSchemasAndHints(t *testing.T) {
	session, _ := connectGoSDK(t, orderSchema)
	tools, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(tools.Tools) != 1 {
		t.Fatalf("advertised %d tools, want 1", len(tools.Tools))
	}
	advertised := tools.Tools[0]
	if advertised.Name != "publish_order" {
		t.Errorf("name = %q", advertised.Name)
	}
	if advertised.InputSchema == nil {
		t.Error("inputSchema is missing from the advertised tool")
	}
	if advertised.OutputSchema == nil {
		t.Error("outputSchema is missing from the advertised tool")
	}
	if advertised.Annotations == nil {
		t.Fatal("annotations are missing from the advertised tool")
	}
	if advertised.Annotations.DestructiveHint == nil || !*advertised.Annotations.DestructiveHint {
		t.Error("destructiveHint is not true in the advertised tool")
	}
	if advertised.Annotations.IdempotentHint {
		t.Error("idempotentHint is true, but publishing twice appends two records")
	}
}

func TestWrapGoSDKPublishesAValidCall(t *testing.T) {
	session, publisher := connectGoSDK(t, orderSchema)
	result, err := session.CallTool(context.Background(), &gomcp.CallToolParams{
		Name: "publish_order", Arguments: map[string]any{"id": "o-1", "amount": 12.5},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("tool result = %#v", result.Content)
	}
	if string(publisher.event.Key) != "o-1" || publisher.event.Topic != "orders.created" {
		t.Fatalf("published event = %#v", publisher.event)
	}
}

// The go-sdk method this adapter uses never validates arguments, so without
// the runtime's own check these calls would reach the encoder — and a field
// the schema forbids would simply be dropped.
func TestWrapGoSDKRejectsArgumentsOutsideTheAdvertisedSchema(t *testing.T) {
	for _, call := range []struct {
		name      string
		arguments map[string]any
	}{
		{"hallucinated field", map[string]any{"id": "o-1", "amount": 12.5, "currency": "GBP"}},
		{"wrong type", map[string]any{"id": 42, "amount": 12.5}},
		{"missing required field", map[string]any{"id": "o-1"}},
	} {
		t.Run(call.name, func(t *testing.T) {
			session, publisher := connectGoSDK(t, orderSchema)
			result, err := session.CallTool(context.Background(), &gomcp.CallToolParams{Name: "publish_order", Arguments: call.arguments})
			if err != nil {
				t.Fatal(err)
			}
			if !result.IsError {
				t.Fatalf("call was accepted; published %#v", publisher.event)
			}
			if publisher.event.Topic != "" {
				t.Fatalf("a rejected call still published %#v", publisher.event)
			}
		})
	}
}
