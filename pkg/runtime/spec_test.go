package runtime

import (
	"reflect"
	"testing"
)

// These tests publish every schema-declaration construct of the Avro
// specification that V1 supports through a registered tool, then decode the
// record and compare it field by field. They cover 1.12.0 and note where
// 1.10.x and 1.11.x differ.
// https://avro.apache.org/docs/1.12.0/specification/

type specCase struct {
	fields, arguments string
	want              map[string]any
	// decodeWith overrides the schema used to read the record back, for
	// declarations the decoder interprets differently from the encoder.
	decodeWith string
}

func (c specCase) run(t *testing.T) {
	t.Helper()
	schema := `{"type":"record","name":"R","namespace":"p","fields":[` + c.fields + `]}`
	result, publisher := publishThroughTool(t, schema, c.arguments)
	if result.IsError {
		t.Fatalf("tool result = %s", result.Error)
	}
	reader := schema
	if c.decodeWith != "" {
		reader = `{"type":"record","name":"R","namespace":"p","fields":[` + c.decodeWith + `]}`
	}
	got := decodePublished(t, reader, publisher)
	for field, want := range c.want {
		if !reflect.DeepEqual(got[field], want) {
			t.Errorf("%s = %#v, want %#v", field, got[field], want)
		}
	}
}

func TestSpecPrimitivesRoundTrip(t *testing.T) {
	for name, c := range map[string]specCase{
		"null":    {`{"name":"f","type":"null"}`, `{"f":null}`, map[string]any{"f": nil}, ""},
		"boolean": {`{"name":"f","type":"boolean"}`, `{"f":true}`, map[string]any{"f": true}, ""},
		"int":     {`{"name":"min","type":"int"},{"name":"max","type":"int"}`, `{"min":-2147483648,"max":2147483647}`, map[string]any{"min": int32(-2147483648), "max": int32(2147483647)}, ""},
		"long":    {`{"name":"min","type":"long"},{"name":"max","type":"long"}`, `{"min":-9223372036854775808,"max":9223372036854775807}`, map[string]any{"min": int64(-9223372036854775808), "max": int64(9223372036854775807)}, ""},
		"float":   {`{"name":"f","type":"float"}`, `{"f":0.25}`, map[string]any{"f": float32(0.25)}, ""},
		"double":  {`{"name":"f","type":"double"}`, `{"f":-1e-300}`, map[string]any{"f": -1e-300}, ""},
		"bytes":   {`{"name":"f","type":"bytes"},{"name":"empty","type":"bytes"}`, `{"f":"AP8=","empty":""}`, map[string]any{"f": []byte{0x00, 0xff}, "empty": []byte{}}, ""},
		"string":  {`{"name":"f","type":"string"},{"name":"empty","type":"string"}`, `{"f":"héllo ✓ 😀","empty":""}`, map[string]any{"f": "héllo ✓ 😀", "empty": ""}, ""},
		// "Primitive type names are also defined type names": {"type":"int"}
		// is the same schema as "int".
		"object form": {`{"name":"i","type":{"type":"int"}},{"name":"b","type":{"type":"bytes"}}`, `{"i":7,"b":"AQ=="}`, map[string]any{"i": int32(7), "b": []byte{1}}, ""},
	} {
		t.Run(name, c.run)
	}
}

// Each row of the specification's "field default values" table: the JSON
// type a default is written in, and the value the encoder must substitute
// when the caller omits the field.
func TestSpecFieldDefaultsForEveryType(t *testing.T) {
	for name, c := range map[string]specCase{
		"null":    {`{"name":"f","type":"null","default":null}`, `{}`, map[string]any{"f": nil}, ""},
		"boolean": {`{"name":"f","type":"boolean","default":true}`, `{}`, map[string]any{"f": true}, ""},
		"int":     {`{"name":"f","type":"int","default":1}`, `{}`, map[string]any{"f": int32(1)}, ""},
		"long":    {`{"name":"f","type":"long","default":9007199254740993}`, `{}`, map[string]any{"f": int64(9007199254740993)}, ""},
		"float":   {`{"name":"f","type":"float","default":1.1}`, `{}`, map[string]any{"f": float32(1.1)}, ""},
		"double":  {`{"name":"f","type":"double","default":1.1}`, `{}`, map[string]any{"f": 1.1}, ""},
		// A bytes default is a JSON string whose code points 0-255 are the
		// byte values, not UTF-8.
		"bytes":  {`{"name":"f","type":"bytes","default":"ÿ\u0000A"}`, `{}`, map[string]any{"f": []byte{0xff, 0x00, 'A'}}, ""},
		"string": {`{"name":"f","type":"string","default":"foo"}`, `{}`, map[string]any{"f": "foo"}, ""},
		"record": {`{"name":"f","type":{"type":"record","name":"X","fields":[{"name":"a","type":"int"}]},"default":{"a":1}}`, `{}`, map[string]any{"f": map[string]any{"a": int32(1)}}, ""},
		"enum":   {`{"name":"f","type":{"type":"enum","name":"E","symbols":["FOO","BAR"]},"default":"BAR"}`, `{}`, map[string]any{"f": "BAR"}, ""},
		"array":  {`{"name":"f","type":{"type":"array","items":"int"},"default":[1]}`, `{}`, map[string]any{"f": []any{int32(1)}}, ""},
		"map":    {`{"name":"f","type":{"type":"map","values":"int"},"default":{"a":1}}`, `{}`, map[string]any{"f": map[string]any{"a": int32(1)}}, ""},
		// A supplied value still wins over the default.
		"supplied value": {`{"name":"f","type":"string","default":"foo"}`, `{"f":"bar"}`, map[string]any{"f": "bar"}, ""},
	} {
		t.Run(name, c.run)
	}
}

func TestSpecUnions(t *testing.T) {
	for name, c := range map[string]specCase{
		"null first, value":  {`{"name":"f","type":["null","string"]}`, `{"f":"a"}`, map[string]any{"f": "a"}, ""},
		"null first, null":   {`{"name":"f","type":["null","string"]}`, `{"f":null}`, map[string]any{"f": nil}, ""},
		"null second, value": {`{"name":"f","type":["string","null"]}`, `{"f":"a"}`, map[string]any{"f": "a"}, ""},
		"null second, null":  {`{"name":"f","type":["string","null"]}`, `{"f":null}`, map[string]any{"f": nil}, ""},
		"bytes branch":       {`{"name":"f","type":["bytes","null"]}`, `{"f":"AQI="}`, map[string]any{"f": []byte{1, 2}}, ""},
		"record branch":      {`{"name":"f","type":["null",{"type":"record","name":"X","fields":[{"name":"a","type":"long"}]}]}`, `{"f":{"a":5}}`, map[string]any{"f": map[string]any{"a": int64(5)}}, ""},
		"enum branch":        {`{"name":"f","type":["null",{"type":"enum","name":"E","symbols":["A","B"]}]}`, `{"f":"B"}`, map[string]any{"f": "B"}, ""},
		"array branch":       {`{"name":"f","type":["null",{"type":"array","items":"string"}]}`, `{"f":["a"]}`, map[string]any{"f": []any{"a"}}, ""},
		"map branch":         {`{"name":"f","type":["null",{"type":"map","values":"string"}]}`, `{"f":{"k":"v"}}`, map[string]any{"f": map[string]any{"k": "v"}}, ""},
		// 1.10.x and 1.11.x: "Default values for union fields correspond to
		// the first schema in the union."
		"default matches first branch (null)":   {`{"name":"f","type":["null","string"],"default":null}`, `{}`, map[string]any{"f": nil}, ""},
		"default matches first branch (string)": {`{"name":"f","type":["string","null"],"default":"x"}`, `{}`, map[string]any{"f": "x"}, ""},
		// 1.12.0 relaxes that: the default "must match with one element of
		// the union", not necessarily the first.
		"default matches a later branch (1.12)": {`{"name":"f","type":["null","string"],"default":"x"}`, `{}`, map[string]any{"f": "x"}, ""},
	} {
		t.Run(name, c.run)
	}
}

func TestSpecComplexTypes(t *testing.T) {
	for name, c := range map[string]specCase{
		"array of arrays": {`{"name":"f","type":{"type":"array","items":{"type":"array","items":"int"}}}`, `{"f":[[1,2],[]]}`,
			map[string]any{"f": []any{[]any{int32(1), int32(2)}, []any{}}}, ""},
		"array of nullable": {`{"name":"f","type":{"type":"array","items":["null","string"]}}`, `{"f":[null,"a"]}`,
			map[string]any{"f": []any{nil, "a"}}, ""},
		"empty array": {`{"name":"f","type":{"type":"array","items":"string"}}`, `{"f":[]}`, map[string]any{"f": []any{}}, ""},
		"map of maps": {`{"name":"f","type":{"type":"map","values":{"type":"map","values":"long"}}}`, `{"f":{"a":{"b":1}}}`,
			map[string]any{"f": map[string]any{"a": map[string]any{"b": int64(1)}}}, ""},
		"map with unusual keys": {`{"name":"f","type":{"type":"map","values":"int"}}`, `{"f":{"":1,"with space":2,"ü":3}}`,
			map[string]any{"f": map[string]any{"": int32(1), "with space": int32(2), "ü": int32(3)}}, ""},
		"empty record": {`{"name":"f","type":{"type":"record","name":"Nothing","fields":[]}}`, `{"f":{}}`,
			map[string]any{"f": map[string]any{}}, ""},
		"enum with doc and reader default": {`{"name":"f","type":{"type":"enum","name":"E","doc":"d","symbols":["A","B"],"default":"A"}}`, `{"f":"B"}`,
			map[string]any{"f": "B"}, ""},
		"record, field aliases and order": {`{"name":"f","aliases":["g"],"order":"ignore","type":{"type":"record","name":"X","aliases":["OldX"],"fields":[{"name":"a","type":"string","order":"descending"}]}}`, `{"f":{"a":"v"}}`,
			map[string]any{"f": map[string]any{"a": "v"}}, ""},
	} {
		t.Run(name, c.run)
	}
}

// Recursive types reached through each kind of container.
func TestSpecRecursion(t *testing.T) {
	for name, c := range map[string]specCase{
		"through a union": {`{"name":"n","type":{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]}]}}`,
			`{"n":{"next":{"next":null}}}`, map[string]any{"n": map[string]any{"next": map[string]any{"next": nil}}}, ""},
		"through an array": {`{"name":"t","type":{"type":"record","name":"Tree","fields":[{"name":"kids","type":{"type":"array","items":"Tree"}}]}}`,
			`{"t":{"kids":[{"kids":[]}]}}`, map[string]any{"t": map[string]any{"kids": []any{map[string]any{"kids": []any{}}}}}, ""},
		"through a map": {`{"name":"g","type":{"type":"record","name":"Graph","fields":[{"name":"edges","type":{"type":"map","values":"Graph"}}]}}`,
			`{"g":{"edges":{"a":{"edges":{}}}}}`, map[string]any{"g": map[string]any{"edges": map[string]any{"a": map[string]any{"edges": map[string]any{}}}}}, ""},
	} {
		t.Run(name, c.run)
	}
}

// Names: a dotted name is a fullname whatever namespace is also given; a bare
// name inherits the most tightly enclosing namespace; references resolve by
// either form; names are case-sensitive.
func TestSpecNames(t *testing.T) {
	for name, c := range map[string]specCase{
		"dotted name ignores its namespace attribute": {`{"name":"a","type":{"type":"record","name":"q.X","namespace":"ignored","fields":[{"name":"v","type":"bytes"}]}},{"name":"b","type":"q.X"}`,
			`{"a":{"v":"AQ=="},"b":{"v":"Ag=="}}`, map[string]any{"a": map[string]any{"v": []byte{1}}, "b": map[string]any{"v": []byte{2}}}, ""},
		// Inside a dotted-name record, a bare reference binds to the fullname's
		// namespace (q), never to the ignored attribute.
		"bare reference inside a dotted-name record": {`{"name":"a","type":{"type":"record","name":"q.X","namespace":"ignored","fields":[{"name":"in","type":{"type":"record","name":"In","fields":[{"name":"v","type":"bytes"}]}},{"name":"again","type":"In"}]}}`,
			`{"a":{"in":{"v":"AQ=="},"again":{"v":"Ag=="}}}`, map[string]any{"a": map[string]any{"in": map[string]any{"v": []byte{1}}, "again": map[string]any{"v": []byte{2}}}}, ""},
		"enum inherits the enclosing record's namespace": {`{"name":"a","type":{"type":"record","name":"In","namespace":"q","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["A"]}}]}},{"name":"b","type":"q.E"}`,
			`{"a":{"e":"A"},"b":"A"}`, map[string]any{"a": map[string]any{"e": "A"}, "b": "A"}, ""},
		"explicit null namespace": {`{"name":"a","type":{"type":"record","name":"Top","namespace":"","fields":[{"name":"v","type":"bytes"}]}},{"name":"b","type":"Top"}`,
			`{"a":{"v":"AQ=="},"b":{"v":"Ag=="}}`, map[string]any{"a": map[string]any{"v": []byte{1}}, "b": map[string]any{"v": []byte{2}}}, ""},
		"names differing only in case are distinct": {`{"name":"a","type":{"type":"record","name":"Case","fields":[{"name":"v","type":"int"}]}},{"name":"b","type":{"type":"record","name":"case","fields":[{"name":"v","type":"string"}]}}`,
			`{"a":{"v":1},"b":{"v":"s"}}`, map[string]any{"a": map[string]any{"v": int32(1)}, "b": map[string]any{"v": "s"}}, ""},
	} {
		t.Run(name, c.run)
	}
}

// Annotations the specification says to treat as the underlying type: an
// unknown logical type, a logical type on the wrong underlying type, and a
// logicalType attribute placed on the field instead of its type.
func TestSpecNonLogicalTypeAnnotationsEncodeAsTheUnderlyingType(t *testing.T) {
	for name, c := range map[string]specCase{
		"unknown logical type on long":  {`{"name":"f","type":{"type":"long","logicalType":"my-custom"}}`, `{"f":9007199254740993}`, map[string]any{"f": int64(9007199254740993)}, ""},
		"unknown logical type on bytes": {`{"name":"f","type":{"type":"bytes","logicalType":"my-custom"}}`, `{"f":"AQI="}`, map[string]any{"f": []byte{1, 2}}, ""},
		"uuid on int":                   {`{"name":"f","type":{"type":"int","logicalType":"uuid"}}`, `{"f":7}`, map[string]any{"f": int32(7)}, ""},
		// twmb/avro's decoder reads a field-level logicalType as a timestamp,
		// so read the record back with the plain declaration it denotes.
		"logicalType on the field": {`{"name":"f","type":"long","logicalType":"timestamp-millis"}`, `{"f":1}`, map[string]any{"f": int64(1)},
			`{"name":"f","type":"long"}`},
	} {
		t.Run(name, c.run)
	}
}

// An enum's "default" is for schema resolution at read time; it must not let
// a writer publish a symbol the enum does not declare.
func TestSpecEnumDefaultDoesNotAdmitUnknownSymbols(t *testing.T) {
	const schema = `{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}}]}`
	if result, publisher := publishThroughTool(t, schema, `{"f":"C"}`); !result.IsError || publisher.event.Topic != "" {
		t.Fatalf("an undeclared symbol was published: %#v", result)
	}
}
