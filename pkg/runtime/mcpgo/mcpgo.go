// Package mcpgo adapts a mark3labs/mcp-go server to the runtime's SDK-neutral
// MCPServer. It lives in its own package so that consumers compile only the
// MCP SDK they actually use.
package mcpgo

import (
	"context"
	"encoding/json"

	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/runtime"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

type adapter struct{ server *server.MCPServer }

func Wrap(server *server.MCPServer) runtime.MCPServer { return &adapter{server: server} }

func (a *adapter) AddTool(tool runtime.ToolDefinition, handler runtime.ToolHandler) {
	destructive, idempotent := tool.Annotations.DestructiveHint, tool.Annotations.IdempotentHint
	definition := mcp.NewToolWithRawSchema(tool.Name, tool.Description, tool.InputSchema)
	definition.RawOutputSchema = tool.OutputSchema
	definition.Annotations = mcp.ToolAnnotation{DestructiveHint: &destructive, IdempotentHint: &idempotent}
	a.server.AddTool(definition, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		// GetArguments has already unmarshalled the payload, turning every
		// number into a float64; re-marshalling it would hand the runtime a
		// lossy copy. RawArguments is the untouched wire JSON, which is what
		// CallToolRequest.Arguments is defined to carry.
		arguments, ok := request.GetRawArguments().(json.RawMessage)
		if !ok {
			var err error
			if arguments, err = json.Marshal(request.Params.Arguments); err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
		}
		result := handler(ctx, runtime.CallToolRequest{Arguments: arguments})
		if result.IsError {
			return mcp.NewToolResultError(result.Error), nil
		}
		return mcp.NewToolResultJSON(result.StructuredContent)
	})
}
