package jsonschema

import (
	"encoding/json"
	"testing"
)

func TestConvertRecordUsesDefaultsAndNullableFieldsForRequired(t *testing.T) {
	schema, err := Convert([]byte(`{
  "type":"record", "name":"OrderCreated", "namespace":"orders.v1",
  "fields":[
    {"name":"orderId", "type":"string"},
    {"name":"note", "type":["null","string"], "default":null},
    {"name":"status", "type":{"type":"enum","name":"Status","symbols":["NEW","PAID"]}},
    {"name":"items", "type":{"type":"array","items":{"type":"record","name":"Item","fields":[{"name":"sku","type":"string"}]}}}
  ]
}`))
	if err != nil {
		t.Fatalf("Convert() error = %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(schema, &got); err != nil {
		t.Fatalf("generated JSON is invalid: %v", err)
	}
	required := got["required"].([]any)
	if len(required) != 3 || required[0] != "orderId" || required[1] != "status" || required[2] != "items" {
		t.Fatalf("required = %#v", required)
	}
	if got["$schema"] != "https://json-schema.org/draft/2020-12/schema" {
		t.Fatalf("$schema = %v", got["$schema"])
	}
}
