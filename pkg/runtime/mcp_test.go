package runtime

import (
	"context"
	"encoding/json"
	"testing"
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
