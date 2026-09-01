package runtime

import (
	"context"
	"encoding/json"

	gomcp "github.com/modelcontextprotocol/go-sdk/mcp"
)

func RegisterGoSDK(server *gomcp.Server, service *Service, tool Tool, inputSchema json.RawMessage, description string) {
	server.AddTool(&gomcp.Tool{Name: tool.Name, Description: description, InputSchema: inputSchema}, func(ctx context.Context, request *gomcp.CallToolRequest) (*gomcp.CallToolResult, error) {
		var payload map[string]any
		if err := json.Unmarshal(request.Params.Arguments, &payload); err != nil {
			return &gomcp.CallToolResult{IsError: true}, nil
		}
		result, err := service.Publish(ctx, tool, payload)
		if err != nil {
			return &gomcp.CallToolResult{IsError: true}, nil
		}
		return &gomcp.CallToolResult{StructuredContent: result}, nil
	})
}
