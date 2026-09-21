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
	destructive := tool.Annotations.DestructiveHint
	definition := mcp.NewToolWithRawSchema(tool.Name, tool.Description, tool.InputSchema)
	definition.RawOutputSchema = tool.OutputSchema
	definition.Annotations = mcp.ToolAnnotation{DestructiveHint: &destructive}
	a.server.AddTool(definition, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		arguments, err := json.Marshal(request.GetArguments())
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		result := handler(ctx, CallToolRequest{Arguments: arguments})
		if result.IsError {
			return mcp.NewToolResultError(result.Error), nil
		}
		return mcp.NewToolResultJSON(result.StructuredContent)
	})
}
