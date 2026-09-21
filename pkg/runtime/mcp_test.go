package runtime

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/jsonschema"
	"github.com/mark3labs/mcp-go/server"
	"github.com/twmb/avro"
)

type serverStub struct {
	tool    ToolDefinition
	handler ToolHandler
}

func (s *serverStub) AddTool(tool ToolDefinition, handler ToolHandler) {
	s.tool, s.handler = tool, handler
}

func TestRegisterToolExposesSchemasAndReturnsStructuredPublishResult(t *testing.T) {
	server := &serverStub{}
	publisher := &publisherStub{}
	service := NewService(resolverStub{id: 9}, publisher)
	RegisterTool(server, service, Tool{Name: "publish_order", Topic: "orders.created", Subject: "orders.created-value", Schema: []byte(`{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`)}, json.RawMessage(`{"type":"object"}`), "Publish an order.")
	if server.tool.Name != "publish_order" || len(server.tool.OutputSchema) == 0 || !server.tool.Annotations.DestructiveHint {
		t.Fatalf("tool definition = %#v", server.tool)
	}
	result := server.handler(context.Background(), CallToolRequest{Arguments: json.RawMessage(`{"id":"o-1"}`)})
	if result.IsError || result.StructuredContent == nil {
		t.Fatalf("tool result = %#v", result)
	}
}

// The mcp-go adapter must carry the output schema and the destructive hint
// through to the wire representation, not just the name and input schema.
func TestWrapMCPGoPreservesOutputSchemaAndDestructiveHint(t *testing.T) {
	mcpServer := server.NewMCPServer("test", "0.0.1")
	service := NewService(resolverStub{id: 9}, &publisherStub{})
	RegisterTool(WrapMCPGo(mcpServer), service, Tool{Name: "publish_order", Topic: "orders.created", Subject: "orders.created-value", Schema: []byte(`{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`)}, json.RawMessage(`{"type":"object"}`), "Publish an order.")

	registered, ok := mcpServer.ListTools()["publish_order"]
	if !ok {
		t.Fatal("publish_order was not registered")
	}
	encoded, err := json.Marshal(registered.Tool)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		InputSchema  json.RawMessage `json:"inputSchema"`
		OutputSchema json.RawMessage `json:"outputSchema"`
		Annotations  struct {
			DestructiveHint *bool `json:"destructiveHint"`
			IdempotentHint  *bool `json:"idempotentHint"`
		} `json:"annotations"`
	}
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	if len(wire.InputSchema) == 0 {
		t.Error("inputSchema is missing from the advertised tool")
	}
	if len(wire.OutputSchema) == 0 {
		t.Errorf("outputSchema is missing from the advertised tool: %s", encoded)
	}
	if wire.Annotations.DestructiveHint == nil || !*wire.Annotations.DestructiveHint {
		t.Errorf("annotations.destructiveHint is not true in the advertised tool: %s", encoded)
	}
	// Publishing twice appends two records, and the hint is a *bool behind
	// omitempty: a nil would drop it from the wire entirely.
	if wire.Annotations.IdempotentHint == nil || *wire.Annotations.IdempotentHint {
		t.Errorf("annotations.idempotentHint is not false in the advertised tool: %s", encoded)
	}
}

// The runtime decodes arguments with UseNumber so that a long beyond
// float64's exact-integer range survives. That only holds if the adapter
// hands over the original wire bytes: mcp-go's GetArguments has already
// unmarshalled every number into a float64.
func TestWrapMCPGoPreservesIntegerPrecisionFromTheWire(t *testing.T) {
	const schema = `{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}`
	input, err := jsonschema.Convert([]byte(schema))
	if err != nil {
		t.Fatal(err)
	}
	mcpServer := server.NewMCPServer("test", "0.0.1")
	publisher := &publisherStub{}
	RegisterTool(WrapMCPGo(mcpServer), NewService(resolverStub{id: 1}, publisher),
		Tool{Name: "publish", Topic: "orders.created", Subject: "orders.created-value", Schema: []byte(schema)},
		json.RawMessage(input), "Publish.")

	const id = 9007199254740993 // 2^53 + 1
	response := mcpServer.HandleMessage(context.Background(), json.RawMessage(
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"publish","arguments":{"id":9007199254740993}}}`))
	if response == nil {
		t.Fatal("no response from the mcp-go server")
	}
	if publisher.event.Topic == "" {
		encoded, _ := json.Marshal(response)
		t.Fatalf("nothing was published: %s", encoded)
	}
	avroSchema, err := avro.Parse(schema)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if _, err := avroSchema.Decode(publisher.event.Value[5:], &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["id"] != int64(id) {
		t.Fatalf("published id = %v, want %d (the wire value, not its float64 rounding)", decoded["id"], int64(id))
	}
}
