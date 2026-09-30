package runtime

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/jsonschema"
)

// orderPlacedSchema is the example's complex event: every V1 construct in one
// realistic record, rather than each in isolation.
const orderPlacedSchema = "../../examples/orders/order-placed.avsc"

// registerOrderPlaced registers the complex event exactly as the generator
// would, key rule included, and returns its schema text and a call function.
func registerOrderPlaced(t *testing.T, publisher Publisher) (string, func(string) ToolResult) {
	t.Helper()
	schema, err := os.ReadFile(orderPlacedSchema)
	if err != nil {
		t.Fatal(err)
	}
	input, err := jsonschema.Convert(schema)
	if err != nil {
		t.Fatalf("Convert() error = %v", err)
	}
	if input, err = jsonschema.MarkKey(input, "orderId"); err != nil {
		t.Fatalf("MarkKey() error = %v", err)
	}
	server := &serverStub{}
	RegisterTool(server, NewService(resolverStub{id: 1}, publisher),
		Tool{Name: "publish_order_placed", Topic: "orders.placed", Subject: "orders.placed-value", KeyField: "orderId", Schema: schema},
		json.RawMessage(input), "Publish an order.")
	return string(schema), func(arguments string) ToolResult {
		return server.handler(context.Background(), CallToolRequest{Arguments: json.RawMessage(arguments)})
	}
}

func TestRegisterToolRoundTripsEveryFieldOfTheComplexSchema(t *testing.T) {
	publisher := &publisherStub{}
	schema, call := registerOrderPlaced(t, publisher)
	result := call(`{
		"orderId": "o-1",
		"placedAt": 9007199254740993,
		"status": "CONFIRMED",
		"previousStatus": "PENDING",
		"customer": {"id": "c-1", "email": "a@example.com",
			"address": {"line1": "1 High St", "line2": null, "city": "London", "postcode": "N1 1AA", "country": "GB"}},
		"shippingAddress": {"line1": "2 Low Rd", "city": "Leeds", "postcode": "LS1 1AA"},
		"lines": [
			{"sku": "s-1", "quantity": 2, "unitPriceMinor": 1250, "attributes": {"size": "M"}, "discount": {"code": "TEN", "amountMinor": 125}},
			{"sku": "s-2", "quantity": 1, "unitPriceMinor": 500}
		],
		"tags": ["gift"],
		"metadata": {"channel": "web", "campaign": null},
		"notes": ["leave at door"],
		"attachments": {"receipt": "aGVsbG8="},
		"signature": "c2ln",
		"category": {"name": "shirts", "parent": {"name": "tops", "parent": {"name": "clothing", "parent": null}}},
		"priority": 1.5,
		"giftWrap": true,
		"total": 26.25
	}`)
	if result.IsError {
		t.Fatalf("tool result = %s", result.Error)
	}
	if string(publisher.event.Key) != "o-1" || publisher.event.Topic != "orders.placed" {
		t.Fatalf("published event = %#v", publisher.event)
	}

	want := map[string]any{
		"orderId":        "o-1",
		"placedAt":       int64(9007199254740993), // 2^53+1 survives intact
		"status":         "CONFIRMED",
		"previousStatus": "PENDING",
		"customer": map[string]any{"id": "c-1", "email": "a@example.com",
			"address": map[string]any{"line1": "1 High St", "line2": nil, "city": "London", "postcode": "N1 1AA", "country": "GB"}},
		// Omitted nested fields take their defaults inside a union branch too.
		"shippingAddress": map[string]any{"line1": "2 Low Rd", "line2": nil, "city": "Leeds", "postcode": "LS1 1AA", "country": "GB"},
		"lines": []any{
			map[string]any{"sku": "s-1", "quantity": int32(2), "unitPriceMinor": int64(1250),
				"attributes": map[string]any{"size": "M"}, "discount": map[string]any{"code": "TEN", "amountMinor": int64(125)}},
			map[string]any{"sku": "s-2", "quantity": int32(1), "unitPriceMinor": int64(500),
				"attributes": map[string]any{}, "discount": nil},
		},
		"tags":        []any{"gift"},
		"metadata":    map[string]any{"channel": "web", "campaign": nil},
		"notes":       []any{"leave at door"},
		"attachments": map[string]any{"receipt": []byte("hello")}, // decoded from base64, not the text
		"signature":   []byte("sig"),
		"category": map[string]any{"name": "shirts", "parent": map[string]any{"name": "tops",
			"parent": map[string]any{"name": "clothing", "parent": nil}}},
		"priority": float32(1.5),
		"giftWrap": true,
		"total":    26.25,
	}
	got := decodePublished(t, schema, publisher)
	for field, expected := range want {
		if !reflect.DeepEqual(got[field], expected) {
			t.Errorf("%s = %#v, want %#v", field, got[field], expected)
		}
	}
	if len(got) != len(want) {
		t.Errorf("decoded %d fields, want %d", len(got), len(want))
	}
}

// Only the fields without defaults are sent; the encoder must fill in the
// rest, including nulls for the optional unions.
func TestRegisterToolFillsComplexSchemaDefaults(t *testing.T) {
	publisher := &publisherStub{}
	schema, call := registerOrderPlaced(t, publisher)
	result := call(`{"orderId":"o-2","placedAt":1,"status":"PENDING",
		"customer":{"id":"c-2","address":{"line1":"1 High St","city":"London","postcode":"N1"}},
		"lines":[],"metadata":{},"category":{"name":"misc"},"priority":0,"total":0}`)
	if result.IsError {
		t.Fatalf("tool result = %s", result.Error)
	}
	got := decodePublished(t, schema, publisher)
	for field, expected := range map[string]any{
		"previousStatus":  nil,
		"shippingAddress": nil,
		"notes":           nil,
		"signature":       nil,
		"tags":            []any{},
		"attachments":     map[string]any{},
		"giftWrap":        false,
	} {
		if !reflect.DeepEqual(got[field], expected) {
			t.Errorf("%s = %#v, want default %#v", field, got[field], expected)
		}
	}
	if country := got["customer"].(map[string]any)["address"].(map[string]any)["country"]; country != "GB" {
		t.Errorf("customer.address.country = %#v, want default GB", country)
	}
}

// Each mistake sits somewhere the flat tests never reach: inside a union
// branch, an array element, a map value, or a nested record.
func TestRegisterToolRejectsMistakesDeepInTheComplexSchema(t *testing.T) {
	const valid = `"placedAt":1,"status":"PENDING","customer":{"id":"c","address":{"line1":"l","city":"c","postcode":"p"}},"metadata":{},"category":{"name":"n"},"priority":0,"total":0`
	// Every case below is this valid call with one mistake; if the base ever
	// stops being valid, the table would pass for the wrong reason.
	if _, call := registerOrderPlaced(t, &publisherStub{}); call(`{"orderId":"o",` + valid + `,"lines":[]}`).IsError {
		t.Fatal("the shared valid payload was rejected")
	}
	for name, arguments := range map[string]string{
		"empty key":                           `{"orderId":"",` + valid + `,"lines":[]}`,
		"unknown enum symbol through a union": `{"orderId":"o",` + valid + `,"lines":[],"previousStatus":"LOST"}`,
		"unknown field inside a union record": `{"orderId":"o",` + valid + `,"lines":[{"sku":"s","quantity":1,"unitPriceMinor":1,"discount":{"code":"X","amountMinor":1,"percent":5}}]}`,
		"int overflow in an array element":    `{"orderId":"o",` + valid + `,"lines":[{"sku":"s","quantity":2147483648,"unitPriceMinor":1}]}`,
		"string where a record belongs":       `{"orderId":"o",` + valid + `,"lines":["s-1"]}`,
		"invalid base64 in a map value":       `{"orderId":"o",` + valid + `,"lines":[],"attachments":{"x":"not base64!"}}`,
		"number as a nullable map value":      `{"orderId":"o",` + valid + `,"lines":[],"metadata":{"k":1}}`,
		"missing field in a recursive parent": `{"orderId":"o",` + valid + `,"lines":[],"category":{"name":"n","parent":{"parent":null}}}`,
		"null for a non-nullable record": `{"orderId":"o","placedAt":1,"status":"PENDING","customer":{"id":"c","address":null},` +
			`"lines":[],"metadata":{},"category":{"name":"n"},"priority":0,"total":0}`,
	} {
		t.Run(name, func(t *testing.T) {
			publisher := &publisherStub{}
			_, call := registerOrderPlaced(t, publisher)
			if result := call(arguments); !result.IsError {
				t.Fatal("the call was accepted")
			}
			if publisher.event.Topic != "" {
				t.Fatalf("a rejected call still published %#v", publisher.event)
			}
		})
	}
}
