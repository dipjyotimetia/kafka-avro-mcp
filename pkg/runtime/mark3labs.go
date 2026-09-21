package runtime

import (
	"context"
	"encoding/json"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

type mcpGoAdapter struct{ server *server.MCPServer }

func WrapMCPGo(server *server.MCPServer) MCPServer { return &mcpGoAdapter{server: server} }

func (a *mcpGoAdapter) AddTool(tool ToolDefinition, handler ToolHandler) {
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
		result := handler(ctx, CallToolRequest{Arguments: arguments})
		if result.IsError {
			return mcp.NewToolResultError(result.Error), nil
		}
		return mcp.NewToolResultJSON(result.StructuredContent)
	})
}
