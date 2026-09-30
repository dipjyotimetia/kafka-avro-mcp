// Package gosdk adapts an official modelcontextprotocol/go-sdk server to the
// runtime's SDK-neutral MCPServer. It lives in its own package so that
// consumers compile only the MCP SDK they actually use.
package gosdk

import (
	"context"
	"encoding/json"

	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/runtime"
	gomcp "github.com/modelcontextprotocol/go-sdk/mcp"
)

type adapter struct{ server *gomcp.Server }

func Wrap(server *gomcp.Server) runtime.MCPServer { return &adapter{server: server} }

func (a *adapter) AddTool(tool runtime.ToolDefinition, handler runtime.ToolHandler) {
	destructive := tool.Annotations.DestructiveHint
	annotations := &gomcp.ToolAnnotations{DestructiveHint: &destructive, IdempotentHint: tool.Annotations.IdempotentHint}
	a.server.AddTool(&gomcp.Tool{Name: tool.Name, Description: tool.Description, InputSchema: tool.InputSchema, OutputSchema: tool.OutputSchema, Annotations: annotations}, func(ctx context.Context, request *gomcp.CallToolRequest) (*gomcp.CallToolResult, error) {
		result := handler(ctx, runtime.CallToolRequest{Arguments: request.Params.Arguments})
		response := &gomcp.CallToolResult{IsError: result.IsError, StructuredContent: result.StructuredContent}
		if result.IsError {
			response.Content = []gomcp.Content{&gomcp.TextContent{Text: result.Error}}
			return response, nil
		}
		// The low-level AddTool leaves Content empty, unlike the generic one,
		// so hosts that read only content would see nothing. Mirror the
		// structured result there as JSON, as the spec suggests.
		text, err := json.Marshal(result.StructuredContent)
		if err != nil {
			return nil, err
		}
		response.Content = []gomcp.Content{&gomcp.TextContent{Text: string(text)}}
		return response, nil
	})
}
