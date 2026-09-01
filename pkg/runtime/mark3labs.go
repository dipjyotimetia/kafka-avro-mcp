package runtime

import (
	"context"
	"encoding/json"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func RegisterMCPGo(s *server.MCPServer, service *Service, tool Tool, inputSchema json.RawMessage, description string) {
	s.AddTool(mcp.NewToolWithRawSchema(tool.Name, description, inputSchema), func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		result, err := service.Publish(ctx, tool, request.GetArguments())
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return mcp.NewToolResultJSON(result)
	})
}
