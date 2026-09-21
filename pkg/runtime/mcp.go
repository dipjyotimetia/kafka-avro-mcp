package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/twmb/avro"
)

type ToolAnnotations struct {
	DestructiveHint bool `json:"destructiveHint"`
	// IdempotentHint is false for every publish tool: a repeated call appends
	// another record. Agents retry tool calls, so saying so explicitly is
	// worth more than leaving the client to assume a default.
	IdempotentHint bool `json:"idempotentHint"`
}
type ToolDefinition struct {
	Name         string          `json:"name"`
	Description  string          `json:"description"`
	InputSchema  json.RawMessage `json:"inputSchema"`
	OutputSchema json.RawMessage `json:"outputSchema"`
	Annotations  ToolAnnotations `json:"annotations"`
}

// CallToolRequest carries the arguments exactly as they arrived on the wire.
// Adapters must not unmarshal and re-marshal them: RegisterTool decodes with
// UseNumber so that integers beyond float64's exact range survive, and a
// round trip through map[string]any would quietly undo that.
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

// RegisterTool advertises tool and enforces its input schema. It panics if the
// input schema or the embedded Avro schema does not parse: both are generated
// constants, so a failure is a defect worth surfacing at start-up rather than
// on the first call.
func RegisterTool(server MCPServer, service *Service, tool Tool, input json.RawMessage, description string) {
	// Parsed once here rather than on every publish: avro.Parse builds a fresh
	// schema cache per call, so leaving it on the hot path re-parses the same
	// constant for the lifetime of the server.
	parsed, err := avro.Parse(string(tool.Schema))
	if err != nil {
		panic(fmt.Errorf("tool %q: parse embedded Avro schema: %w", tool.Name, err))
	}
	schema := newArguments(tool.Name, input, parsed)
	definition := ToolDefinition{
		Name:         tool.Name,
		Description:  description,
		InputSchema:  input,
		OutputSchema: publishResultSchema,
		Annotations:  ToolAnnotations{DestructiveHint: true, IdempotentHint: false},
	}
	server.AddTool(definition, func(ctx context.Context, request CallToolRequest) ToolResult {
		payload, err := schema.decode(request.Arguments)
		if err != nil {
			return ToolResult{IsError: true, Error: err.Error()}
		}
		result, err := service.publish(ctx, tool, parsed, payload)
		if err != nil {
			return ToolResult{IsError: true, Error: toolError(service, tool, err)}
		}
		return ToolResult{StructuredContent: result}
	})
}

// toolError decides what a failure may tell the model. A payload problem is
// the model's to fix, so it travels verbatim; anything else can carry broker
// hostnames or registry addresses and stays in the log.
func toolError(service *Service, tool Tool, err error) string {
	var payload PayloadError
	if errors.As(err, &payload) {
		return err.Error()
	}
	service.logger.Error("publishing an MCP tool call failed", "tool", tool.Name, "topic", tool.Topic, "subject", tool.Subject, "error", err)
	return fmt.Sprintf("publishing to %q failed; the server log has the details", tool.Topic)
}
