// Package jsonschema converts the V1 Avro subset into JSON Schema 2020-12.
package jsonschema

import (
	"encoding/json"
	"fmt"

	"github.com/twmb/avro"
)

const draft202012 = "https://json-schema.org/draft/2020-12/schema"

type avroRecord struct {
	Type      json.RawMessage `json:"type"`
	Name      string          `json:"name"`
	Namespace string          `json:"namespace"`
	Doc       string          `json:"doc"`
	Fields    []avroField     `json:"fields"`
	Symbols   []string        `json:"symbols"`
	Items     json.RawMessage `json:"items"`
	Values    json.RawMessage `json:"values"`
	Logical   string          `json:"logicalType"`
}

type avroField struct {
	Name    string          `json:"name"`
	Type    json.RawMessage `json:"type"`
	Doc     string          `json:"doc"`
	Default json.RawMessage `json:"default"`
}

type converter struct {
	defs map[string]any
}

func Convert(schemaJSON []byte) ([]byte, error) {
	if _, err := avro.Parse(string(schemaJSON)); err != nil {
		return nil, fmt.Errorf("parse Avro schema: %w", err)
	}
	var root avroRecord
	if err := json.Unmarshal(schemaJSON, &root); err != nil {
		return nil, fmt.Errorf("decode Avro schema: %w", err)
	}
	if kind, _ := primitive(root.Type); kind != "record" {
		return nil, fmt.Errorf("V1 root schema must be an Avro record")
	}
	c := converter{defs: map[string]any{}}
	converted, err := c.record(root, true)
	if err != nil {
		return nil, err
	}
	result := converted.(map[string]any)
	result["$schema"] = draft202012
	if len(c.defs) > 0 {
		result["$defs"] = c.defs
	}
	return json.Marshal(result)
}

func (c *converter) record(record avroRecord, root bool) (any, error) {
	properties := make(map[string]any, len(record.Fields))
	required := make([]string, 0, len(record.Fields))
	for _, field := range record.Fields {
		value, nullable, err := c.typeSchema(field.Type)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", field.Name, err)
		}
		if field.Doc != "" {
			value.(map[string]any)["description"] = field.Doc
		}
		properties[field.Name] = value
		if len(field.Default) == 0 && !nullable {
			required = append(required, field.Name)
		}
	}
	result := map[string]any{"type": "object", "properties": properties, "additionalProperties": false}
	if record.Doc != "" {
		result["description"] = record.Doc
	}
	if len(required) > 0 {
		result["required"] = required
	}
	if !root && record.Name != "" {
		c.defs[record.Name] = result
		return map[string]any{"$ref": "#/$defs/" + record.Name}, nil
	}
	return result, nil
}

func (c *converter) typeSchema(raw json.RawMessage) (any, bool, error) {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		if schema, ok := primitiveSchema(text); ok {
			return schema, false, nil
		}
		return map[string]any{"$ref": "#/$defs/" + text}, false, nil
	}
	var union []json.RawMessage
	if json.Unmarshal(raw, &union) == nil {
		if len(union) != 2 {
			return nil, false, fmt.Errorf("only nullable two-branch unions are supported in V1")
		}
		var nonNull json.RawMessage
		nulls := 0
		for _, branch := range union {
			var name string
			if json.Unmarshal(branch, &name) == nil && name == "null" {
				nulls++
			} else {
				nonNull = branch
			}
		}
		if nulls != 1 {
			return nil, false, fmt.Errorf("only nullable unions are supported in V1")
		}
		schema, _, err := c.typeSchema(nonNull)
		if err != nil {
			return nil, false, err
		}
		return map[string]any{"anyOf": []any{map[string]any{"type": "null"}, schema}}, true, nil
	}
	var complex avroRecord
	if err := json.Unmarshal(raw, &complex); err != nil {
		return nil, false, fmt.Errorf("unsupported Avro type")
	}
	kind, ok := primitive(complex.Type)
	if !ok {
		return nil, false, fmt.Errorf("unsupported Avro type")
	}
	if complex.Logical != "" {
		return nil, false, fmt.Errorf("logical type %q is not supported in V1", complex.Logical)
	}
	switch kind {
	case "record":
		v, err := c.record(complex, false)
		return v, false, err
	case "enum":
		return map[string]any{"type": "string", "enum": complex.Symbols}, false, nil
	case "array":
		items, _, err := c.typeSchema(complex.Items)
		if err != nil {
			return nil, false, err
		}
		return map[string]any{"type": "array", "items": items}, false, nil
	case "map":
		values, _, err := c.typeSchema(complex.Values)
		if err != nil {
			return nil, false, err
		}
		return map[string]any{"type": "object", "additionalProperties": values}, false, nil
	default:
		schema, ok := primitiveSchema(kind)
		if !ok {
			return nil, false, fmt.Errorf("unsupported Avro type %q", kind)
		}
		return schema, false, nil
	}
}

func primitive(raw json.RawMessage) (string, bool) {
	var text string
	return text, json.Unmarshal(raw, &text) == nil
}

func primitiveSchema(kind string) (map[string]any, bool) {
	switch kind {
	case "null":
		return map[string]any{"type": "null"}, true
	case "boolean":
		return map[string]any{"type": "boolean"}, true
	case "int", "long":
		return map[string]any{"type": "integer"}, true
	case "float", "double":
		return map[string]any{"type": "number"}, true
	case "string":
		return map[string]any{"type": "string"}, true
	case "bytes":
		return map[string]any{"type": "string", "contentEncoding": "base64"}, true
	default:
		return nil, false
	}
}
