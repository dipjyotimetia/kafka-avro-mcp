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
	a.server.AddTool(mcp.NewToolWithRawSchema(tool.Name, tool.Description, tool.InputSchema), func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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
