package jsonschema

import (
	"encoding/json"
	"strings"
	"testing"
)

// These tests walk the schema-declaration rules of the Avro specification —
// 1.12.0, with the differences from 1.11.x and 1.10.x called out — and pin
// down what the converter does with each: accept and describe it, reject it
// as invalid Avro, or reject it as outside the V1 subset.
// https://avro.apache.org/docs/1.12.0/specification/

// field wraps a single field declaration in a record.
func field(declaration string) string {
	return `{"type":"record","name":"R","namespace":"p","fields":[` + declaration + `]}`
}

// Schemas the specification forbids. avro.Parse enforces most of these; the
// point is that none reaches a generated tool.
func TestSpecRejectsInvalidAvro(t *testing.T) {
	for rule, schema := range map[string]string{
		"Names: field name must match [A-Za-z_][A-Za-z0-9_]*":    field(`{"name":"1bad","type":"string"}`),
		"Names: record name must match the name grammar":         `{"type":"record","name":"a-b","fields":[]}`,
		"Names: namespace is dot-separated names, none empty":    `{"type":"record","name":"R","namespace":"a..b","fields":[]}`,
		"Names: primitive names may not be defined":              field(`{"name":"f","type":{"type":"record","name":"string","fields":[]}}`),
		"Names: a fullname may be defined only once":             field(`{"name":"a","type":{"type":"record","name":"X","fields":[]}},{"name":"b","type":{"type":"record","name":"X","fields":[]}}`),
		"Names: references are to previously defined names":      field(`{"name":"a","type":"X"},{"name":"b","type":{"type":"record","name":"X","fields":[]}}`),
		"Records: field names are unique":                        field(`{"name":"f","type":"string"},{"name":"f","type":"int"}`),
		"Enums: every symbol must match the name grammar":        field(`{"name":"f","type":{"type":"enum","name":"E","symbols":["1A"]}}`),
		"Enums: symbols must be unique":                          field(`{"name":"f","type":{"type":"enum","name":"E","symbols":["A","A"]}}`),
		"Unions: may not immediately contain other unions":       field(`{"name":"f","type":["null",["string"]]}`),
		"Unions: no two schemas of the same unnamed type":        field(`{"name":"f","type":["string","string"]}`),
		"Unions: null may appear only once":                      field(`{"name":"f","type":["null","null"]}`),
		"References: an undefined name does not resolve":         field(`{"name":"f","type":"Missing"}`),
		"Logical types: invalid decimal (parser is stricter[1])": field(`{"name":"f","type":{"type":"bytes","logicalType":"decimal","precision":2,"scale":5}}`),
	} {
		t.Run(rule, func(t *testing.T) {
			if _, err := Convert([]byte(schema)); err == nil {
				t.Fatal("Convert() accepted a schema the specification forbids")
			}
		})
	}
	// [1] The specification says an invalid logical type "should" be ignored
	// in favour of its underlying type; twmb/avro rejects it at parse time
	// instead, so such a schema is refused rather than silently reinterpreted.
}

// Valid Avro that V1 deliberately does not map to a tool. Each must fail with
// a message that names the V1 limit, not an obscure downstream error.
func TestSpecRejectsValidAvroOutsideV1(t *testing.T) {
	cases := map[string]string{
		"Fixed":                               field(`{"name":"f","type":{"type":"fixed","name":"F","size":2}}`),
		"Fixed inside a union":                field(`{"name":"f","type":["null",{"type":"fixed","name":"F","size":2}]}`),
		"Unions: a single branch":             field(`{"name":"f","type":["string"]}`),
		"Unions: two non-null branches":       field(`{"name":"f","type":["string","long"]}`),
		"Unions: three branches":              field(`{"name":"f","type":["null","string","long"]}`),
		"Unions: two named records":           field(`{"name":"f","type":[{"type":"record","name":"A","fields":[]},{"type":"record","name":"B","fields":[]}]}`),
		"Logical type inside a nullable":      field(`{"name":"f","type":["null",{"type":"long","logicalType":"timestamp-millis"}]}`),
		"Logical type as array items":         field(`{"name":"f","type":{"type":"array","items":{"type":"int","logicalType":"date"}}}`),
		"Logical type as map values":          field(`{"name":"f","type":{"type":"map","values":{"type":"string","logicalType":"uuid"}}}`),
		"Logical type in a nested record":     field(`{"name":"f","type":{"type":"record","name":"In","fields":[{"name":"g","type":{"type":"long","logicalType":"time-micros"}}]}}`),
		"Logical type on the key-shaped type": field(`{"name":"id","type":{"type":"string","logicalType":"uuid"}}`),
	}
	// Every logical type each specification version defines, on its declared
	// underlying type. 1.12.0 added timestamp-nanos, local-timestamp-nanos,
	// big-decimal and uuid on fixed(16); 1.10.x and 1.11.x define the rest.
	for name, declaration := range map[string]string{
		"decimal on bytes (1.10+)":              `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
		"decimal on fixed (1.10+)":              `{"type":"fixed","name":"D","size":4,"logicalType":"decimal","precision":4}`,
		"big-decimal on bytes (1.12)":           `{"type":"bytes","logicalType":"big-decimal"}`,
		"uuid on string (1.10+)":                `{"type":"string","logicalType":"uuid"}`,
		"uuid on fixed(16) (1.12)":              `{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}`,
		"date (1.10+)":                          `{"type":"int","logicalType":"date"}`,
		"time-millis (1.10+)":                   `{"type":"int","logicalType":"time-millis"}`,
		"time-micros (1.10+)":                   `{"type":"long","logicalType":"time-micros"}`,
		"timestamp-millis (1.10+)":              `{"type":"long","logicalType":"timestamp-millis"}`,
		"timestamp-micros (1.10+)":              `{"type":"long","logicalType":"timestamp-micros"}`,
		"timestamp-nanos (1.12)":                `{"type":"long","logicalType":"timestamp-nanos"}`,
		"local-timestamp-millis (1.10+)":        `{"type":"long","logicalType":"local-timestamp-millis"}`,
		"local-timestamp-micros (1.10+)":        `{"type":"long","logicalType":"local-timestamp-micros"}`,
		"local-timestamp-nanos (1.12)":          `{"type":"long","logicalType":"local-timestamp-nanos"}`,
		"duration on fixed(12) (1.10+)":         `{"type":"fixed","name":"Du","size":12,"logicalType":"duration"}`,
		"timestamp-millis in a record's record": `{"type":"record","name":"Deep","fields":[{"name":"at","type":{"type":"long","logicalType":"timestamp-millis"}}]}`,
	} {
		cases["Logical types: "+name] = field(`{"name":"f","type":` + declaration + `}`)
	}
	for rule, schema := range cases {
		t.Run(rule, func(t *testing.T) {
			_, err := Convert([]byte(schema))
			if err == nil {
				t.Fatal("Convert() accepted a construct outside V1")
			}
			if !strings.Contains(err.Error(), "V1") && !strings.Contains(err.Error(), "unsupported") {
				t.Fatalf("error %q does not say the construct is unsupported", err)
			}
		})
	}
}

// "Language implementations must ignore unknown logical types when reading,
// and should use the underlying Avro type. If a logical type is invalid ...
// implementations should ignore the logical type and use the underlying Avro
// type." A logicalType attribute on a field, rather than on its type, is not
// a logical type at all. All three leave an ordinary primitive.
func TestSpecTreatsNonLogicalTypeAnnotationsAsTheUnderlyingType(t *testing.T) {
	for rule, tc := range map[string]struct{ declaration, want string }{
		"unknown logical type on string": {`{"name":"f","type":{"type":"string","logicalType":"my-custom"}}`, `{"type":"string"}`},
		"unknown logical type on long":   {`{"name":"f","type":{"type":"long","logicalType":"my-custom"}}`, `{"maximum":9223372036854775807,"minimum":-9223372036854775808,"type":"integer"}`},
		"unknown logical type on bytes":  {`{"name":"f","type":{"type":"bytes","logicalType":"my-custom"}}`, `{"contentEncoding":"base64","type":"string"}`},
		"uuid on int (wrong underlying)": {`{"name":"f","type":{"type":"int","logicalType":"uuid"}}`, `{"maximum":2147483647,"minimum":-2147483648,"type":"integer"}`},
		"date on string (wrong type)":    {`{"name":"f","type":{"type":"string","logicalType":"date"}}`, `{"type":"string"}`},
		"logicalType on the field":       {`{"name":"f","type":"long","logicalType":"timestamp-millis"}`, `{"maximum":9223372036854775807,"minimum":-9223372036854775808,"type":"integer"}`},
	} {
		t.Run(rule, func(t *testing.T) {
			converted, err := Convert([]byte(field(tc.declaration)))
			if err != nil {
				t.Fatalf("Convert() error = %v", err)
			}
			if want := `"f":` + tc.want; !strings.Contains(string(converted), want) {
				t.Fatalf("converted %s, want property %s", converted, want)
			}
		})
	}
}

// Declarations that are valid Avro and inside V1, checked for the JSON Schema
// they produce.
func TestSpecDescribesValidDeclarations(t *testing.T) {
	for rule, tc := range map[string]struct{ declaration, want string }{
		"Primitives: null":                          {`{"name":"f","type":"null"}`, `{"type":"null"}`},
		"Primitives: boolean":                       {`{"name":"f","type":"boolean"}`, `{"type":"boolean"}`},
		"Primitives: float":                         {`{"name":"f","type":"float"}`, `{"type":"number"}`},
		"Primitives: double":                        {`{"name":"f","type":"double"}`, `{"type":"number"}`},
		"Primitives: string":                        {`{"name":"f","type":"string"}`, `{"type":"string"}`},
		"Primitives: object form equals name form":  {`{"name":"f","type":{"type":"string"}}`, `{"type":"string"}`},
		"Primitives: object form of int":            {`{"name":"f","type":{"type":"int"}}`, `{"maximum":2147483647,"minimum":-2147483648,"type":"integer"}`},
		"Records: aliases and order are ignored":    {`{"name":"f","type":"string","aliases":["g"],"order":"descending"}`, `{"type":"string"}`},
		"Enums: doc becomes the description":        {`{"name":"f","type":{"type":"enum","name":"E","doc":"Colour.","symbols":["RED","BLUE"]}}`, `{"description":"Colour.","enum":["RED","BLUE"],"type":"string"}`},
		"Enums: default is a reader-side attribute": {`{"name":"f","type":{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}}`, `{"enum":["A","B"],"type":"string"}`},
		"Arrays: of arrays":                         {`{"name":"f","type":{"type":"array","items":{"type":"array","items":"string"}}}`, `{"items":{"items":{"type":"string"},"type":"array"},"type":"array"}`},
		"Arrays: of nullable items":                 {`{"name":"f","type":{"type":"array","items":["null","string"]}}`, `{"items":{"anyOf":[{"type":"null"},{"type":"string"}]},"type":"array"}`},
		"Maps: of maps":                             {`{"name":"f","type":{"type":"map","values":{"type":"map","values":"string"}}}`, `{"additionalProperties":{"additionalProperties":{"type":"string"},"type":"object"},"type":"object"}`},
		"Unions: null may come second":              {`{"name":"f","type":["string","null"]}`, `{"anyOf":[{"type":"null"},{"type":"string"}]}`},
	} {
		t.Run(rule, func(t *testing.T) {
			converted, err := Convert([]byte(field(tc.declaration)))
			if err != nil {
				t.Fatalf("Convert() error = %v", err)
			}
			if want := `"f":` + tc.want; !strings.Contains(string(converted), want) {
				t.Fatalf("converted %s, want property %s", converted, want)
			}
		})
	}
}

// "Records: ... aliases" and "doc" on the record itself, and a record with no
// fields at all, are all valid.
func TestSpecAcceptsRecordLevelAttributesAndEmptyRecords(t *testing.T) {
	converted, err := Convert([]byte(`{"type":"record","name":"R","aliases":["Old"],"doc":"Root.","fields":[{"name":"empty","type":{"type":"record","name":"Nothing","fields":[]}}]}`))
	if err != nil {
		t.Fatalf("Convert() error = %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(converted, &got); err != nil {
		t.Fatal(err)
	}
	if got["description"] != "Root." {
		t.Errorf("description = %v, want the record doc", got["description"])
	}
	nothing := got["$defs"].(map[string]any)["Nothing"].(map[string]any)
	if _, ok := nothing["required"]; ok || nothing["additionalProperties"] != false {
		t.Errorf("empty record = %v, want a closed object with nothing required", nothing)
	}
}

// Names: "If the name specified contains a dot, then it is assumed to be a
// fullname, and any namespace also specified is ignored", and equality of
// names is case-sensitive.
func TestSpecNameResolution(t *testing.T) {
	got := convert(t, `{"type":"record","name":"R","namespace":"p","fields":[
		{"name":"dotted","type":{"type":"record","name":"q.X","namespace":"ignored","fields":[]}},
		{"name":"byFullname","type":"q.X"},
		{"name":"upper","type":{"type":"record","name":"Case","fields":[]}},
		{"name":"lower","type":{"type":"record","name":"case","fields":[]}}
	]}`)
	assertRefsResolve(t, got)
	defs := got["$defs"].(map[string]any)
	for _, name := range []string{"q.X", "p.Case", "p.case"} {
		if _, ok := defs[name]; !ok {
			t.Errorf("$defs lacks %q; holds %v", name, keys(defs))
		}
	}
	if _, ok := defs["ignored.q.X"]; ok {
		t.Error("the namespace attribute was applied to a dotted name")
	}
}

// Primitive names are equivalent in either form, so the key rule must accept
// {"type":"string"} as readily as "string".
func TestSpecKeyAcceptsEitherFormOfString(t *testing.T) {
	for _, declaration := range []string{`"string"`, `{"type":"string"}`, `{"type":"string","logicalType":"my-custom"}`} {
		if err := ValidateKey("id", []byte(field(`{"name":"id","type":`+declaration+`}`))); err != nil {
			t.Errorf("ValidateKey(%s) error = %v", declaration, err)
		}
	}
	for _, declaration := range []string{`{"type":"int"}`, `["null","string"]`, `{"type":"enum","name":"E","symbols":["A"]}`} {
		if err := ValidateKey("id", []byte(field(`{"name":"id","type":`+declaration+`}`))); err == nil {
			t.Errorf("ValidateKey(%s) accepted a non-string key", declaration)
		}
	}
}
