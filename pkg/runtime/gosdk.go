package runtime

import (
	"context"

	gomcp "github.com/modelcontextprotocol/go-sdk/mcp"
)

type goSDKAdapter struct{ server *gomcp.Server }

func WrapGoSDK(server *gomcp.Server) MCPServer { return &goSDKAdapter{server: server} }

func (a *goSDKAdapter) AddTool(tool ToolDefinition, handler ToolHandler) {
	destructive := tool.Annotations.DestructiveHint
	a.server.AddTool(&gomcp.Tool{Name: tool.Name, Description: tool.Description, InputSchema: tool.InputSchema, OutputSchema: tool.OutputSchema, Annotations: &gomcp.ToolAnnotations{DestructiveHint: &destructive}}, func(ctx context.Context, request *gomcp.CallToolRequest) (*gomcp.CallToolResult, error) {
		result := handler(ctx, CallToolRequest{Arguments: request.Params.Arguments})
		response := &gomcp.CallToolResult{IsError: result.IsError, StructuredContent: result.StructuredContent}
		if result.IsError {
			response.Content = []gomcp.Content{&gomcp.TextContent{Text: result.Error}}
		}
		return response, nil
	})
}
