package runtime

import (
	"context"
	"encoding/json"
)

type ToolAnnotations struct {
	DestructiveHint bool `json:"destructiveHint"`
}
type ToolDefinition struct {
	Name         string          `json:"name"`
	Description  string          `json:"description"`
	InputSchema  json.RawMessage `json:"inputSchema"`
	OutputSchema json.RawMessage `json:"outputSchema"`
	Annotations  ToolAnnotations `json:"annotations"`
}
type CallToolRequest struct{ Arguments json.RawMessage }
type ToolResult struct {
	StructuredContent any    `json:"structuredContent,omitempty"`
	IsError           bool   `json:"isError,omitempty"`
	Error             string `json:"error,omitempty"`
}
type ToolHandler func(context.Context, CallToolRequest) ToolResult
type MCPServer interface {
	AddTool(ToolDefinition, ToolHandler)
}

var publishResultSchema = json.RawMessage(`{"type":"object","properties":{"topic":{"type":"string"},"partition":{"type":"integer"},"offset":{"type":"integer"},"schemaId":{"type":"integer"},"timestamp":{"type":"string","format":"date-time"}},"required":["topic","partition","offset","schemaId","timestamp"],"additionalProperties":false}`)

func RegisterTool(server MCPServer, service *Service, tool Tool, input json.RawMessage, description string) {
	server.AddTool(ToolDefinition{Name: tool.Name, Description: description, InputSchema: input, OutputSchema: publishResultSchema, Annotations: ToolAnnotations{DestructiveHint: true}}, func(ctx context.Context, request CallToolRequest) ToolResult {
		var payload map[string]any
		if err := json.Unmarshal(request.Arguments, &payload); err != nil {
			return ToolResult{IsError: true, Error: "invalid tool arguments: " + err.Error()}
		}
		result, err := service.Publish(ctx, tool, payload)
		if err != nil {
			return ToolResult{IsError: true, Error: err.Error()}
		}
		return ToolResult{StructuredContent: result}
	})
}
