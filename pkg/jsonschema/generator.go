// Package jsonschema converts the V1 Avro subset into JSON Schema 2020-12 and
// validates the Avro-level constraints the generated tools rely on.
package jsonschema

import (
	"encoding/json"
	"fmt"
	"maps"
	"strings"

	"github.com/twmb/avro"
)

const draft202012 = "https://json-schema.org/draft/2020-12/schema"

type avroRecord struct {
	Type json.RawMessage `json:"type"`
	Name string          `json:"name"`
	// Namespace is a pointer so that an absent attribute (inherit the
	// enclosing namespace) stays distinct from an explicit "" (the null
	// namespace). Avro treats those differently and accepts a schema that
	// uses both for the same short name.
	Namespace *string         `json:"namespace"`
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
	// defs holds every named type encountered, keyed by its resolved Avro
	// fullname. Keying by fullname (rather than the bare name) is what keeps
	// two same-named records in different namespaces from colliding.
	defs map[string]any
	// used records the fullnames an emitted $ref actually points at, so that
	// $defs carries exactly the referenced types and Convert can fail on a
	// reference nothing defines.
	used map[string]bool
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
	c := converter{defs: map[string]any{}, used: map[string]bool{}}
	converted, err := c.record(root, "", true)
	if err != nil {
		return nil, err
	}
	result := converted.(map[string]any)
	result["$schema"] = draft202012

	// Every $ref must resolve, or the tool advertises a schema no client can
	// follow. Avro accepts these (avro.Parse ran above), so this is the only
	// place a dangling reference can be caught before it ships to a model.
	defs := make(map[string]any, len(c.used))
	for name := range c.used {
		body, ok := c.defs[name]
		if !ok {
			return nil, fmt.Errorf("unresolved Avro type reference %q", name)
		}
		defs[name] = body
	}
	if len(defs) > 0 {
		result["$defs"] = defs
	}
	return json.Marshal(result)
}

func (c *converter) record(record avroRecord, enclosing string, root bool) (any, error) {
	// Register the body before walking fields so a record that references
	// itself resolves against a definition that already exists. The map is
	// filled in place below, so the registered entry ends up complete.
	body := map[string]any{}
	name, err := c.define(record.Name, record.Namespace, enclosing, body)
	if err != nil {
		return nil, err
	}
	inner := namespaceOf(name)

	properties := make(map[string]any, len(record.Fields))
	required := make([]string, 0, len(record.Fields))
	for _, field := range record.Fields {
		value, err := c.typeSchema(field.Type, inner)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", field.Name, err)
		}
		if field.Doc != "" {
			// Copy first: an inline enum schema is the same map as its $defs
			// entry, so writing the description straight into it would leak
			// one field's doc onto every other use of that type.
			annotated := maps.Clone(value.(map[string]any))
			annotated["description"] = field.Doc
			value = annotated
		}
		properties[field.Name] = value
		// A field is optional exactly when Avro can supply a value for it,
		// which means it declares a default. Being nullable is not enough:
		// the encoder raises "missing key" for a nullable field with no
		// default, so advertising it optional promises a call that fails.
		if len(field.Default) == 0 {
			required = append(required, field.Name)
		}
	}
	body["type"] = "object"
	body["properties"] = properties
	body["additionalProperties"] = false
	if record.Doc != "" {
		body["description"] = record.Doc
	}
	if len(required) > 0 {
		body["required"] = required
	}
	if root {
		// The root is returned inline (MCP requires a top-level object
		// schema) but stays registered so a self-reference resolves. Clone
		// it so $schema and $defs land on the copy and never recurse into
		// the definition.
		return maps.Clone(body), nil
	}
	c.used[name] = true
	return map[string]any{"$ref": "#/$defs/" + name}, nil
}

func (c *converter) typeSchema(raw json.RawMessage, enclosing string) (any, error) {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		if schema, ok := primitiveSchema(text); ok {
			return schema, nil
		}
		name := c.resolve(text, enclosing)
		c.used[name] = true
		return map[string]any{"$ref": "#/$defs/" + name}, nil
	}
	var union []json.RawMessage
	if json.Unmarshal(raw, &union) == nil {
		if len(union) != 2 {
			return nil, fmt.Errorf("only nullable two-branch unions are supported in V1")
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
			return nil, fmt.Errorf("only nullable unions are supported in V1")
		}
		schema, err := c.typeSchema(nonNull, enclosing)
		if err != nil {
			return nil, err
		}
		return map[string]any{"anyOf": []any{map[string]any{"type": "null"}, schema}}, nil
	}
	var complex avroRecord
	if err := json.Unmarshal(raw, &complex); err != nil {
		return nil, fmt.Errorf("unsupported Avro type")
	}
	kind, ok := primitive(complex.Type)
	if !ok {
		return nil, fmt.Errorf("unsupported Avro type")
	}
	if complex.Logical != "" {
		return nil, fmt.Errorf("logical type %q is not supported in V1", complex.Logical)
	}
	switch kind {
	case "record":
		return c.record(complex, enclosing, false)
	case "enum":
		// Emitted inline, as it always has been, but also registered so a
		// later reference to the same enum has something to resolve against.
		schema := map[string]any{"type": "string", "enum": complex.Symbols}
		if _, err := c.define(complex.Name, complex.Namespace, enclosing, schema); err != nil {
			return nil, err
		}
		return schema, nil
	case "array":
		items, err := c.typeSchema(complex.Items, enclosing)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "array", "items": items}, nil
	case "map":
		values, err := c.typeSchema(complex.Values, enclosing)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "object", "additionalProperties": values}, nil
	default:
		schema, ok := primitiveSchema(kind)
		if !ok {
			return nil, fmt.Errorf("unsupported Avro type %q", kind)
		}
		return schema, nil
	}
}

// define registers a named type under its Avro fullname. avro.Parse already
// rejects a duplicate definition, so the error here only fires if this
// package's naming disagrees with the parser's — which is worth catching,
// because the failure it prevents is one definition silently overwriting
// another.
func (c *converter) define(name string, namespace *string, enclosing string, body any) (string, error) {
	full := fullname(name, namespace, enclosing)
	if _, ok := c.defs[full]; ok {
		return "", fmt.Errorf("duplicate Avro named type %q", full)
	}
	c.defs[full] = body
	return full, nil
}

// resolve turns a named-type reference into the fullname it denotes. A dotted
// reference is already a fullname; a bare one binds to the enclosing namespace
// when that names a known type, and otherwise to the null namespace.
func (c *converter) resolve(text, enclosing string) string {
	if strings.Contains(text, ".") {
		return text
	}
	if enclosing != "" {
		if _, ok := c.defs[enclosing+"."+text]; ok {
			return enclosing + "." + text
		}
	}
	if _, ok := c.defs[text]; ok {
		return text
	}
	if enclosing != "" {
		return enclosing + "." + text
	}
	return text
}

// fullname applies Avro's naming rules: a dotted name is already complete, a
// declared namespace wins over the enclosing one (including an explicit "",
// which selects the null namespace rather than inheriting), and a type left
// with no namespace at all sits in the null namespace.
func fullname(name string, namespace *string, enclosing string) string {
	if strings.Contains(name, ".") {
		return name
	}
	scope := enclosing
	if namespace != nil {
		scope = *namespace
	}
	if scope == "" {
		return name
	}
	return scope + "." + name
}

func namespaceOf(fullname string) string {
	if i := strings.LastIndex(fullname, "."); i >= 0 {
		return fullname[:i]
	}
	return ""
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

// ValidateKey reports whether field can serve as the Kafka message key for the
// given Avro record: it must exist, be a non-null Avro string, and carry no
// default. An empty field means the event is published without a key.
func ValidateKey(field string, schemaJSON []byte) error {
	if field == "" {
		return nil
	}
	var root struct {
		Fields []struct {
			Name    string          `json:"name"`
			Type    json.RawMessage `json:"type"`
			Default json.RawMessage `json:"default"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(schemaJSON, &root); err != nil {
		return err
	}
	for _, candidate := range root.Fields {
		if candidate.Name != field {
			continue
		}
		var typ string
		if json.Unmarshal(candidate.Type, &typ) != nil || typ != "string" {
			return fmt.Errorf("key field %q must be a non-null Avro string", field)
		}
		// A default would make the field optional in the generated schema
		// while the publisher still requires it to derive the message key.
		if len(candidate.Default) != 0 {
			return fmt.Errorf("key field %q must not declare a default", field)
		}
		return nil
	}
	return fmt.Errorf("key field %q does not exist", field)
}
