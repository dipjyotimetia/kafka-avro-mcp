package jsonschema

import (
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"testing"
)

func convert(t *testing.T, schema string) map[string]any {
	t.Helper()
	converted, err := Convert([]byte(schema))
	if err != nil {
		t.Fatalf("Convert() error = %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(converted, &got); err != nil {
		t.Fatalf("generated JSON is invalid: %v", err)
	}
	return got
}

// refs collects every $ref in the document, so a test can assert that the
// generated schema is self-contained rather than eyeballing one pointer.
func refs(node any, into map[string]bool) {
	switch value := node.(type) {
	case map[string]any:
		for key, child := range value {
			if key == "$ref" {
				if reference, ok := child.(string); ok {
					into[reference] = true
				}
				continue
			}
			refs(child, into)
		}
	case []any:
		for _, child := range value {
			refs(child, into)
		}
	}
}

// assertRefsResolve is the invariant every generated schema must hold: a
// dangling $ref is invisible to avro.Parse, so nothing else catches it before
// the schema reaches a model.
func assertRefsResolve(t *testing.T, document map[string]any) map[string]bool {
	t.Helper()
	found := map[string]bool{}
	refs(document, found)
	defs, _ := document["$defs"].(map[string]any)
	for reference := range found {
		name, ok := strings.CutPrefix(reference, "#/$defs/")
		if !ok {
			t.Errorf("unexpected $ref form %q", reference)
			continue
		}
		if _, ok := defs[name]; !ok {
			t.Errorf("$ref %q does not resolve; $defs holds %v", reference, keys(defs))
		}
	}
	return found
}

func keys(m map[string]any) []string { return slices.Sorted(maps.Keys(m)) }

func TestConvertRequiresEveryFieldWithoutAnAvroDefault(t *testing.T) {
	got := convert(t, `{
  "type":"record", "name":"OrderCreated", "namespace":"orders.v1",
  "fields":[
    {"name":"orderId", "type":"string"},
    {"name":"note", "type":["null","string"], "default":null},
    {"name":"tier", "type":"string", "default":"std"},
    {"name":"status", "type":{"type":"enum","name":"Status","symbols":["NEW","PAID"]}},
    {"name":"items", "type":{"type":"array","items":{"type":"record","name":"Item","fields":[{"name":"sku","type":"string"}]}}}
  ]
}`)
	var required []string
	for _, name := range got["required"].([]any) {
		required = append(required, name.(string))
	}
	want := []string{"orderId", "status", "items"}
	if strings.Join(required, ",") != strings.Join(want, ",") {
		t.Fatalf("required = %v, want %v", required, want)
	}
	if got["$schema"] != "https://json-schema.org/draft/2020-12/schema" {
		t.Fatalf("$schema = %v", got["$schema"])
	}
}

// A nullable field with no default is not optional: the Avro encoder raises
// "missing key" for it. Advertising it optional promises a call that fails.
func TestConvertRequiresNullableFieldWithoutDefault(t *testing.T) {
	got := convert(t, `{"type":"record","name":"Order","fields":[
		{"name":"id","type":"string"},
		{"name":"note","type":["null","string"]}
	]}`)
	required := got["required"].([]any)
	if len(required) != 2 || required[1] != "note" {
		t.Fatalf("required = %v, want the nullable field to be required too", required)
	}
}

// Avro lets a named type be referenced by its fullname while the definition
// carries only a short name plus an inherited namespace.
func TestConvertResolvesFullnameReferenceToNestedRecord(t *testing.T) {
	got := convert(t, `{"type":"record","name":"Root","namespace":"r","fields":[
		{"name":"ship","type":{"type":"record","name":"Addr","fields":[{"name":"line","type":"string"}]}},
		{"name":"bill","type":"r.Addr"}
	]}`)
	found := assertRefsResolve(t, got)
	if !found["#/$defs/r.Addr"] {
		t.Fatalf("no reference to the fullname r.Addr; refs = %v", found)
	}
}

// Enums are emitted inline at their definition, so a second reference to one
// has nothing to point at unless the converter registers it.
func TestConvertResolvesSecondReferenceToNamedEnum(t *testing.T) {
	got := convert(t, `{"type":"record","name":"Root","namespace":"r","fields":[
		{"name":"status","type":{"type":"enum","name":"Status","symbols":["NEW","PAID"]}},
		{"name":"previous","type":"r.Status"}
	]}`)
	assertRefsResolve(t, got)
	defs := got["$defs"].(map[string]any)
	enum := defs["r.Status"].(map[string]any)
	if len(enum["enum"].([]any)) != 2 {
		t.Fatalf("registered enum = %#v", enum)
	}
}

// A record that references itself resolves only if the root is registered
// before its own fields are walked.
func TestConvertResolvesRecursiveRecord(t *testing.T) {
	got := convert(t, `{"type":"record","name":"Node","namespace":"r","fields":[
		{"name":"value","type":"string"},
		{"name":"next","type":["null","r.Node"],"default":null}
	]}`)
	assertRefsResolve(t, got)
	if _, ok := got["properties"]; !ok {
		t.Fatal("the root schema must stay an inline object, not a bare $ref")
	}
	if got["type"] != "object" {
		t.Fatalf("root type = %v, want object (MCP requires a top-level object schema)", got["type"])
	}
	// The root is registered before its fields are walked and filled in
	// place, so the definition a self-reference lands on must be the
	// complete record, not the empty placeholder.
	definition := got["$defs"].(map[string]any)["r.Node"].(map[string]any)
	if properties, ok := definition["properties"].(map[string]any); !ok || len(properties) != 2 {
		t.Fatalf("$defs[r.Node] = %#v, want the filled-in record", definition)
	}
}

// Two records sharing a short name in different namespaces are distinct Avro
// types and must not collapse onto one definition.
func TestConvertKeepsSameNamedRecordsInDifferentNamespacesDistinct(t *testing.T) {
	got := convert(t, `{"type":"record","name":"Root","namespace":"r","fields":[
		{"name":"a","type":{"type":"record","name":"Addr","namespace":"x","fields":[{"name":"line","type":"string"}]}},
		{"name":"b","type":{"type":"record","name":"Addr","namespace":"y","fields":[{"name":"code","type":"int"}]}}
	]}`)
	assertRefsResolve(t, got)
	properties := got["properties"].(map[string]any)
	first := properties["a"].(map[string]any)["$ref"]
	second := properties["b"].(map[string]any)["$ref"]
	if first == second {
		t.Fatalf("both records collapsed onto %v", first)
	}
	defs := got["$defs"].(map[string]any)
	if _, ok := defs["x.Addr"]; !ok {
		t.Errorf("$defs is missing x.Addr: %v", keys(defs))
	}
	if _, ok := defs["y.Addr"]; !ok {
		t.Errorf("$defs is missing y.Addr: %v", keys(defs))
	}
}

// A field doc must not leak onto the shared definition of the type it uses.
func TestConvertDoesNotWriteFieldDocOntoSharedEnumDefinition(t *testing.T) {
	got := convert(t, `{"type":"record","name":"Root","namespace":"r","fields":[
		{"name":"status","type":{"type":"enum","name":"Status","symbols":["NEW"]},"doc":"Current status."},
		{"name":"previous","type":"r.Status"}
	]}`)
	if description, ok := got["$defs"].(map[string]any)["r.Status"].(map[string]any)["description"]; ok {
		t.Fatalf("the shared enum definition picked up a field doc: %v", description)
	}
}

func TestValidateKeyRejectsKeyFieldWithDefault(t *testing.T) {
	schema := []byte(`{"type":"record","name":"Order","fields":[{"name":"orderId","type":"string","default":"x"}]}`)
	// A default makes the field optional in the generated schema while the
	// publisher still needs it to derive the message key.
	if err := ValidateKey("orderId", schema); err == nil {
		t.Fatal("ValidateKey accepted a key field with a default")
	}
}

func TestValidateKeyAcceptsPlainStringFieldAndRejectsOthers(t *testing.T) {
	schema := []byte(`{"type":"record","name":"Order","fields":[{"name":"orderId","type":"string"},{"name":"count","type":"int"},{"name":"note","type":["null","string"]}]}`)
	if err := ValidateKey("orderId", schema); err != nil {
		t.Fatalf("ValidateKey() error = %v", err)
	}
	for _, field := range []string{"count", "note", "missing"} {
		if err := ValidateKey(field, schema); err == nil {
			t.Errorf("ValidateKey accepted %q as a message key", field)
		}
	}
}

// Avro distinguishes an absent namespace attribute (inherit the enclosing
// scope) from an explicit "" (the null namespace), and accepts a schema that
// uses both for the same short name.
func TestConvertHonoursExplicitNullNamespace(t *testing.T) {
	got := convert(t, `{"type":"record","name":"Root","namespace":"r","fields":[
		{"name":"a","type":{"type":"record","name":"Addr","namespace":"","fields":[{"name":"line","type":"string"}]}},
		{"name":"b","type":"Addr"}
	]}`)
	assertRefsResolve(t, got)
	properties := got["properties"].(map[string]any)
	if ref := properties["a"].(map[string]any)["$ref"]; ref != "#/$defs/Addr" {
		t.Fatalf("$ref = %v, want the null-namespace name #/$defs/Addr", ref)
	}
	if properties["b"].(map[string]any)["$ref"] != "#/$defs/Addr" {
		t.Fatal("the short reference did not bind to the null-namespace type")
	}
}

func TestConvertKeepsNullNamespaceTypeApartFromInheritedOne(t *testing.T) {
	got := convert(t, `{"type":"record","name":"Root","namespace":"r","fields":[
		{"name":"a","type":{"type":"record","name":"Addr","namespace":"","fields":[{"name":"line","type":"string"}]}},
		{"name":"b","type":{"type":"record","name":"Addr","fields":[{"name":"code","type":"int"}]}}
	]}`)
	assertRefsResolve(t, got)
	defs := got["$defs"].(map[string]any)
	if _, ok := defs["Addr"]; !ok {
		t.Errorf("$defs is missing the null-namespace Addr: %v", keys(defs))
	}
	if _, ok := defs["r.Addr"]; !ok {
		t.Errorf("$defs is missing r.Addr: %v", keys(defs))
	}
}

// Avro int and long are fixed-width, and JSON Schema's integer is not; the
// bounds let an out-of-range value fail validation with a message naming the
// limit instead of reaching the encoder.
func TestConvertAdvertisesIntegerBounds(t *testing.T) {
	converted, err := Convert([]byte(`{"type":"record","name":"E","fields":[{"name":"i","type":"int"},{"name":"l","type":"long"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"i":{"maximum":2147483647,"minimum":-2147483648,"type":"integer"}`,
		`"l":{"maximum":9223372036854775807,"minimum":-9223372036854775808,"type":"integer"}`,
	} {
		if !strings.Contains(string(converted), want) {
			t.Errorf("converted schema %s does not contain %s", converted, want)
		}
	}
}

// The publisher rejects an empty key, so the advertised schema should say so.
func TestMarkKeyRequiresNonEmptyString(t *testing.T) {
	input, err := Convert([]byte(`{"type":"record","name":"E","fields":[{"name":"id","type":"string"},{"name":"note","type":"string"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	marked, err := MarkKey(input, "id")
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Properties map[string]map[string]any `json:"properties"`
	}
	if err := json.Unmarshal(marked, &got); err != nil {
		t.Fatal(err)
	}
	if got.Properties["id"]["minLength"] != float64(1) {
		t.Errorf("key property = %v, want minLength 1", got.Properties["id"])
	}
	if _, ok := got.Properties["note"]["minLength"]; ok {
		t.Errorf("non-key property = %v, want no minLength", got.Properties["note"])
	}
	if unchanged, err := MarkKey(input, ""); err != nil || string(unchanged) != string(input) {
		t.Errorf("MarkKey with no key changed the schema: %s (%v)", unchanged, err)
	}
}
