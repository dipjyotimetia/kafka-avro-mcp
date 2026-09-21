package runtime

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/twmb/avro"
)

// arguments turns raw tool arguments into the payload map the Avro encoder
// expects. Neither MCP adapter enforces the schema it publishes — the go-sdk
// validates only through its generic AddTool, and mcp-go only when the server
// opts in — so the guarantee has to live here, at the one place both adapters
// funnel through.
type arguments struct {
	compiled *jsonschema.Schema
	// root is nil unless the Avro schema contains a bytes field somewhere.
	// Nothing else needs adapting, so the common case skips the walk.
	root  *avro.SchemaNode
	named map[string]*avro.SchemaNode
}

const inputSchemaLocation = "mem:///input.schema.json"

// newArguments panics on an input schema it cannot compile. The schema is a
// generated constant, so a failure is a build-time defect that should surface
// at server start rather than on the first tool call.
func newArguments(toolName string, input json.RawMessage, schema *avro.Schema) *arguments {
	document, err := jsonschema.UnmarshalJSON(bytes.NewReader(input))
	if err != nil {
		panic(fmt.Errorf("tool %q: parse input schema: %w", toolName, err))
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource(inputSchemaLocation, document); err != nil {
		panic(fmt.Errorf("tool %q: add input schema: %w", toolName, err))
	}
	compiled, err := compiler.Compile(inputSchemaLocation)
	if err != nil {
		panic(fmt.Errorf("tool %q: compile input schema: %w", toolName, err))
	}

	a := &arguments{compiled: compiled, named: map[string]*avro.SchemaNode{}}
	root := schema.Root()
	if a.index(root) {
		a.root = root
	} else {
		a.named = nil
	}
	return a
}

// index records every named definition and reports whether the tree contains
// a bytes field. Definitions carry a namespace the parser has already
// resolved, so nothing here re-derives Avro's naming rules.
func (a *arguments) index(node *avro.SchemaNode) bool {
	if node == nil {
		return false
	}
	if node.Name != "" {
		a.named[nodeName(node)] = node
	}
	found := node.Type == "bytes"
	for i := range node.Fields {
		found = a.index(&node.Fields[i].Type) || found
	}
	for i := range node.Branches {
		found = a.index(&node.Branches[i]) || found
	}
	if a.index(node.Items) || a.index(node.Values) {
		found = true
	}
	return found
}

func nodeName(node *avro.SchemaNode) string {
	if node.Namespace == "" {
		return node.Name
	}
	return node.Namespace + "." + node.Name
}

// decode validates the arguments against the advertised schema and converts
// base64 strings into the bytes they denote.
func (a *arguments) decode(raw json.RawMessage) (map[string]any, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = json.RawMessage("{}")
	}
	// UnmarshalJSON decodes with UseNumber. Plain json.Unmarshal would turn
	// every number into a float64, and the Avro encoder rejects any long
	// beyond float64's exact-integer range — which covers ordinary snowflake
	// IDs and nanosecond timestamps.
	document, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("invalid tool arguments: %w", err)
	}
	if err := a.compiled.Validate(document); err != nil {
		return nil, fmt.Errorf("tool arguments do not match the input schema: %s", validationDetail(err))
	}
	if a.root != nil {
		if document, err = a.adapt(a.root, "", document); err != nil {
			return nil, PayloadError{Err: err}
		}
	}
	payload, ok := document.(map[string]any)
	if !ok {
		return nil, PayloadError{Err: errors.New("tool arguments must be a JSON object")}
	}
	return payload, nil
}

// adapt walks the Avro schema alongside the decoded value, converting every
// base64 string at a bytes position into the bytes themselves. The Avro schema
// is what the encoder reads, so driving the conversion from it keeps the two
// from ever disagreeing about which field is binary.
//
// enclosing is the namespace a bare type reference binds to.
func (a *arguments) adapt(node *avro.SchemaNode, enclosing string, value any) (any, error) {
	if definition, ok := a.resolve(node.Type, enclosing); ok {
		node, enclosing = definition, definition.Namespace
	}
	switch node.Type {
	case "bytes":
		text, ok := value.(string)
		if !ok {
			return value, nil
		}
		decoded, err := base64.StdEncoding.DecodeString(text)
		if err != nil {
			return nil, fmt.Errorf("invalid base64 in a bytes field: %w", err)
		}
		return decoded, nil
	case "union":
		if value == nil {
			return nil, nil
		}
		for i := range node.Branches {
			if node.Branches[i].Type == "null" {
				continue
			}
			return a.adapt(&node.Branches[i], enclosing, value)
		}
		return value, nil
	case "record":
		object, ok := value.(map[string]any)
		if !ok {
			return value, nil
		}
		for i := range node.Fields {
			field := &node.Fields[i]
			entry, ok := object[field.Name]
			if !ok {
				continue
			}
			adapted, err := a.adapt(&field.Type, node.Namespace, entry)
			if err != nil {
				return nil, fmt.Errorf("field %q: %w", field.Name, err)
			}
			object[field.Name] = adapted
		}
		return object, nil
	case "array":
		items, ok := value.([]any)
		if !ok || node.Items == nil {
			return value, nil
		}
		for i, entry := range items {
			adapted, err := a.adapt(node.Items, enclosing, entry)
			if err != nil {
				return nil, fmt.Errorf("index %d: %w", i, err)
			}
			items[i] = adapted
		}
		return items, nil
	case "map":
		object, ok := value.(map[string]any)
		if !ok || node.Values == nil {
			return value, nil
		}
		for name, entry := range object {
			adapted, err := a.adapt(node.Values, enclosing, entry)
			if err != nil {
				return nil, fmt.Errorf("key %q: %w", name, err)
			}
			object[name] = adapted
		}
		return object, nil
	}
	return value, nil
}

// resolve follows a named type reference. Only references reach the lookup:
// a definition's Type is its Avro kind, which is never a registered name.
func (a *arguments) resolve(reference, enclosing string) (*avro.SchemaNode, bool) {
	if !strings.Contains(reference, ".") && enclosing != "" {
		if node, ok := a.named[enclosing+"."+reference]; ok {
			return node, true
		}
	}
	node, ok := a.named[reference]
	return node, ok
}

// validationDetail reports the per-field causes without the library's leading
// "failed with <schema url>" line. These messages go to the model verbatim so
// it can correct itself, and an in-memory URI is noise it can do nothing with.
func validationDetail(err error) string {
	var invalid *jsonschema.ValidationError
	if !errors.As(err, &invalid) || len(invalid.Causes) == 0 {
		return err.Error()
	}
	causes := make([]string, 0, len(invalid.Causes))
	for _, cause := range invalid.Causes {
		causes = append(causes, cause.Error())
	}
	return strings.Join(causes, "; ")
}
