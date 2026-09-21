package runtime

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/mark3labs/mcp-go/server"
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
}
