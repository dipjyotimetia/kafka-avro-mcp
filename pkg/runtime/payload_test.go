package runtime

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/jsonschema"
	"github.com/twmb/avro"
)

// publishThroughTool drives the registered MCP handler exactly as an adapter
// would: raw JSON arguments in, a tool result out. The input schema is built
// by the same converter the generator uses, so these exercise the contract a
// generated tool actually advertises.
func publishThroughTool(t *testing.T, avroSchema, arguments string) (ToolResult, *publisherStub) {
	t.Helper()
	publisher := &publisherStub{}
	call := registerTool(t, avroSchema, NewService(resolverStub{id: 1}, publisher))
	return call(arguments), publisher
}

// registerTool registers a tool over the given service and returns a function
// that drives its handler with raw JSON arguments, the way an adapter does.
func registerTool(t *testing.T, avroSchema string, service *Service) func(string) ToolResult {
	t.Helper()
	input, err := jsonschema.Convert([]byte(avroSchema))
	if err != nil {
		t.Fatalf("Convert() error = %v", err)
	}
	server := &serverStub{}
	RegisterTool(server, service,
		Tool{Name: "publish", Topic: "orders.created", Subject: "orders.created-value", Schema: []byte(avroSchema)},
		json.RawMessage(input), "Publish.")
	return func(arguments string) ToolResult {
		return server.handler(context.Background(), CallToolRequest{Arguments: json.RawMessage(arguments)})
	}
}

// decodePublished reads the Avro payload back out of the produced record,
// skipping the 5-byte Confluent wire header.
func decodePublished(t *testing.T, avroSchema string, publisher *publisherStub) map[string]any {
	t.Helper()
	schema, err := avro.Parse(avroSchema)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if _, err := schema.Decode(publisher.event.Value[5:], &decoded); err != nil {
		t.Fatalf("decoding the published record: %v", err)
	}
	return decoded
}

// additionalProperties:false is advertised on every generated tool, but
// neither MCP adapter enforces the schema it publishes and the Avro encoder
// reads only the fields it knows. An unenforced schema means a hallucinated
// field is dropped and the publish still succeeds.
func TestRegisterToolRejectsArgumentsOutsideTheAdvertisedSchema(t *testing.T) {
	const schema = `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`
	result, publisher := publishThroughTool(t, schema, `{"id":"o-1","totl":42}`)
	if !result.IsError {
		t.Fatalf("a hallucinated field was accepted and silently dropped; published %#v", publisher.event)
	}
}

func TestRegisterToolRejectsWronglyTypedArguments(t *testing.T) {
	const schema = `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`
	if result, _ := publishThroughTool(t, schema, `{"id":42}`); !result.IsError {
		t.Fatal("an integer was accepted for an Avro string field")
	}
}

// The schema advertises bytes fields as base64, so the runtime has to decode
// them: the Avro encoder writes a Go string's raw UTF-8 bytes, which would put
// the base64 text on the wire for consumers to decode into garbage.
func TestRegisterToolDecodesBase64BytesRatherThanPublishingTheEncodedText(t *testing.T) {
	const schema = `{"type":"record","name":"Blob","fields":[{"name":"payload","type":"bytes"}]}`
	arguments := `{"payload":"` + base64.StdEncoding.EncodeToString([]byte("hi")) + `"}`
	result, publisher := publishThroughTool(t, schema, arguments)
	if result.IsError {
		t.Fatalf("tool result = %#v", result)
	}
	decoded := decodePublished(t, schema, publisher)
	got, ok := decoded["payload"].([]byte)
	if !ok || string(got) != "hi" {
		t.Fatalf("published bytes = %#v, want the decoded %q", decoded["payload"], "hi")
	}
}

func TestRegisterToolRejectsBytesFieldThatIsNotBase64(t *testing.T) {
	const schema = `{"type":"record","name":"Blob","fields":[{"name":"payload","type":"bytes"}]}`
	if result, _ := publishThroughTool(t, schema, `{"payload":"not base64!!"}`); !result.IsError {
		t.Fatal("a non-base64 string was accepted for a bytes field")
	}
}

// Decoding arguments with plain json.Unmarshal makes every number a float64,
// and the Avro encoder refuses any long outside float64's exact-integer range
// — which is where snowflake IDs and nanosecond timestamps live.
func TestRegisterToolPublishesLongsBeyondFloat64Precision(t *testing.T) {
	const schema = `{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}`
	const id = 9007199254740993 // 2^53 + 1: the first long a float64 cannot hold
	result, publisher := publishThroughTool(t, schema, `{"id":9007199254740993}`)
	if result.IsError {
		t.Fatalf("tool result = %#v", result)
	}
	if got := decodePublished(t, schema, publisher)["id"]; got != int64(id) {
		t.Fatalf("published id = %v (%T), want %d", got, got, int64(id))
	}
}

// A nullable field with no default is not optional: the Avro encoder raises
// "missing key" for it, so the schema must require it rather than promise a
// call that always fails.
func TestRegisterToolRequiresNullableFieldWithoutDefault(t *testing.T) {
	const schema = `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"note","type":["null","string"]}]}`
	if result, _ := publishThroughTool(t, schema, `{"id":"o-1"}`); !result.IsError {
		t.Fatal("a nullable field with no default was advertised as optional")
	}
	result, publisher := publishThroughTool(t, schema, `{"id":"o-1","note":null}`)
	if result.IsError {
		t.Fatalf("an explicit null was rejected: %#v", result)
	}
	if got := decodePublished(t, schema, publisher)["note"]; got != nil {
		t.Fatalf("published note = %#v, want nil", got)
	}
}

// A field with a default is genuinely optional: the encoder fills it in.
func TestRegisterToolAcceptsOmittedFieldWithDefault(t *testing.T) {
	const schema = `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"tier","type":"string","default":"std"}]}`
	result, publisher := publishThroughTool(t, schema, `{"id":"o-1"}`)
	if result.IsError {
		t.Fatalf("tool result = %#v", result)
	}
	if got := decodePublished(t, schema, publisher)["tier"]; got != "std" {
		t.Fatalf("published tier = %#v, want the schema default", got)
	}
}

// Bytes nested inside arrays, maps and nullable unions go through the same
// walk; a decoder that only handled top-level fields would miss them.
func TestRegisterToolDecodesBase64InNestedPositions(t *testing.T) {
	const schema = `{"type":"record","name":"Nested","namespace":"n","fields":[
		{"name":"list","type":{"type":"array","items":"bytes"}},
		{"name":"lookup","type":{"type":"map","values":"bytes"}},
		{"name":"maybe","type":["null","bytes"]},
		{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"blob","type":"bytes"}]}}
	]}`
	encode := base64.StdEncoding.EncodeToString
	arguments := `{"list":["` + encode([]byte("a")) + `"],"lookup":{"k":"` + encode([]byte("b")) + `"},` +
		`"maybe":"` + encode([]byte("c")) + `","inner":{"blob":"` + encode([]byte("d")) + `"}}`
	result, publisher := publishThroughTool(t, schema, arguments)
	if result.IsError {
		t.Fatalf("tool result = %#v", result)
	}
	decoded := decodePublished(t, schema, publisher)
	if got := decoded["list"].([]any)[0].([]byte); string(got) != "a" {
		t.Errorf("array element = %q, want %q", got, "a")
	}
	if got := decoded["lookup"].(map[string]any)["k"].([]byte); string(got) != "b" {
		t.Errorf("map value = %q, want %q", got, "b")
	}
	if got := decoded["maybe"].([]byte); string(got) != "c" {
		t.Errorf("nullable union = %q, want %q", got, "c")
	}
	if got := decoded["inner"].(map[string]any)["blob"].([]byte); string(got) != "d" {
		t.Errorf("nested record field = %q, want %q", got, "d")
	}
}

// An enum is advertised with its symbol list; a value outside it must not
// reach the encoder.
func TestRegisterToolRejectsUnknownEnumSymbol(t *testing.T) {
	const schema = `{"type":"record","name":"Order","fields":[{"name":"status","type":{"type":"enum","name":"Status","symbols":["NEW","PAID"]}}]}`
	if result, _ := publishThroughTool(t, schema, `{"status":"REFUNDED"}`); !result.IsError {
		t.Fatal("a symbol outside the enum was accepted")
	}
}

func TestRegisterToolPanicsOnUninterpretableInputSchema(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("RegisterTool accepted an input schema it cannot compile")
		}
	}()
	RegisterTool(&serverStub{}, NewService(resolverStub{id: 1}, &publisherStub{}),
		Tool{Name: "publish", Topic: "t", Subject: "s"}, json.RawMessage(`{`), "Publish.")
}

// Decoding with UseNumber changes how every numeric field arrives, not just
// the large longs it was introduced for, so the ordinary primitives need to
// keep encoding to the same values.
func TestRegisterToolEncodesOrdinaryPrimitivesAfterNumberDecoding(t *testing.T) {
	const schema = `{"type":"record","name":"Primitives","fields":[
		{"name":"i","type":"int"},
		{"name":"l","type":"long"},
		{"name":"f","type":"float"},
		{"name":"d","type":"double"},
		{"name":"b","type":"boolean"},
		{"name":"s","type":"string"}
	]}`
	result, publisher := publishThroughTool(t, schema, `{"i":-7,"l":42,"f":1.5,"d":12.25,"b":true,"s":"ok"}`)
	if result.IsError {
		t.Fatalf("tool result = %#v", result)
	}
	decoded := decodePublished(t, schema, publisher)
	for _, want := range []struct {
		field string
		value any
	}{{"i", int32(-7)}, {"l", int64(42)}, {"f", float32(1.5)}, {"d", 12.25}, {"b", true}, {"s", "ok"}} {
		if got := decoded[want.field]; got != want.value {
			t.Errorf("%s = %v (%T), want %v (%T)", want.field, got, got, want.value, want.value)
		}
	}
}

// A fractional value for an integer field must be rejected rather than
// truncated on its way to the encoder.
func TestRegisterToolRejectsFractionalValueForIntegerField(t *testing.T) {
	const schema = `{"type":"record","name":"Event","fields":[{"name":"count","type":"int"}]}`
	if result, _ := publishThroughTool(t, schema, `{"count":3.5}`); !result.IsError {
		t.Fatal("a fractional value was accepted for an Avro int field")
	}
}

// A documented field whose type is a named record emits a $ref with a sibling
// description. That is legal in 2020-12, but it is the one shape where the
// validator and the base64 walk both have to look past the annotation.
func TestRegisterToolHandlesDocumentedRefFieldWithSiblingDescription(t *testing.T) {
	const schema = `{"type":"record","name":"Root","namespace":"r","fields":[
		{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"blob","type":"bytes"}]},"doc":"The nested part."}
	]}`
	arguments := `{"inner":{"blob":"` + base64.StdEncoding.EncodeToString([]byte("z")) + `"}}`
	result, publisher := publishThroughTool(t, schema, arguments)
	if result.IsError {
		t.Fatalf("tool result = %#v", result)
	}
	got := decodePublished(t, schema, publisher)["inner"].(map[string]any)["blob"].([]byte)
	if string(got) != "z" {
		t.Fatalf("nested bytes = %q, want %q", got, "z")
	}
	// The annotation must not weaken the schema the field points at.
	if result, _ := publishThroughTool(t, schema, `{"inner":{"blob":"`+base64.StdEncoding.EncodeToString([]byte("z"))+`","extra":1}}`); !result.IsError {
		t.Fatal("a hallucinated field inside a documented $ref was accepted")
	}
}

type failingPublisher struct{ err error }

func (p failingPublisher) Publish(context.Context, Event) (PublishResult, error) {
	return PublishResult{}, p.err
}

// Broker and registry errors carry hostnames and internal addresses. They
// belong in the server log, not in the model's context window.
func TestRegisterToolKeepsInfrastructureDetailOutOfTheToolResult(t *testing.T) {
	const schema = `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`
	var logged bytes.Buffer
	service := NewService(resolverStub{id: 1},
		failingPublisher{err: errors.New("dial tcp broker-3.internal.example:9092: connection refused")},
		WithLogger(slog.New(slog.NewTextHandler(&logged, nil))))
	result := registerTool(t, schema, service)(`{"id":"o-1"}`)

	if !result.IsError {
		t.Fatal("a broker failure was reported as success")
	}
	if strings.Contains(result.Error, "broker-3.internal.example") {
		t.Errorf("the tool result leaked the broker address: %q", result.Error)
	}
	if !strings.Contains(result.Error, "orders.created") {
		t.Errorf("the tool result does not say what failed: %q", result.Error)
	}
	if !strings.Contains(logged.String(), "broker-3.internal.example") {
		t.Errorf("the broker address was dropped instead of logged: %q", logged.String())
	}
}

// A problem the model can actually fix still travels back in full.
func TestRegisterToolReportsPayloadProblemsVerbatim(t *testing.T) {
	const schema = `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"blob","type":"string"}]}`
	var logged bytes.Buffer
	service := NewService(resolverStub{id: 1}, &publisherStub{},
		WithMaxMessageBytes(8), WithLogger(slog.New(slog.NewTextHandler(&logged, nil))))
	result := registerTool(t, schema, service)(`{"id":"o-1","blob":"far too long for the limit"}`)

	if !result.IsError {
		t.Fatal("an oversized record was accepted")
	}
	if !strings.Contains(result.Error, "exceeds") {
		t.Errorf("the size limit was hidden from the caller: %q", result.Error)
	}
	if logged.Len() != 0 {
		t.Errorf("a payload problem was logged as an infrastructure failure: %q", logged.String())
	}
}

// Bytes reached through a named type reference rather than an inline
// definition: the walk has to follow the reference to know the field is
// binary, and a bare name binds to the enclosing namespace.
func TestRegisterToolDecodesBase64ThroughNamedTypeReferences(t *testing.T) {
	const schema = `{"type":"record","name":"Root","namespace":"r","fields":[
		{"name":"first","type":{"type":"record","name":"Blob","fields":[{"name":"data","type":"bytes"}]}},
		{"name":"byFullname","type":"r.Blob"},
		{"name":"byBareName","type":"Blob"}
	]}`
	encode := base64.StdEncoding.EncodeToString
	arguments := `{"first":{"data":"` + encode([]byte("a")) + `"},` +
		`"byFullname":{"data":"` + encode([]byte("b")) + `"},` +
		`"byBareName":{"data":"` + encode([]byte("c")) + `"}}`
	result, publisher := publishThroughTool(t, schema, arguments)
	if result.IsError {
		t.Fatalf("tool result = %#v", result)
	}
	decoded := decodePublished(t, schema, publisher)
	for field, want := range map[string]string{"first": "a", "byFullname": "b", "byBareName": "c"} {
		got := decoded[field].(map[string]any)["data"].([]byte)
		if string(got) != want {
			t.Errorf("%s.data = %q, want %q", field, got, want)
		}
	}
}
