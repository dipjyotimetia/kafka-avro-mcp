package jsonschema

import (
	"os"
	"slices"
	"testing"
)

// orderPlacedSchema is the example's complex event: every V1 construct in one
// realistic record, rather than each in isolation.
const orderPlacedSchema = "../../examples/orders/order-placed.avsc"

func TestConvertComplexOrderPlacedSchema(t *testing.T) {
	schema, err := os.ReadFile(orderPlacedSchema)
	if err != nil {
		t.Fatal(err)
	}
	got := convert(t, string(schema))
	assertRefsResolve(t, got)

	defs := got["$defs"].(map[string]any)
	// Named types across two namespaces, plus the enum referenced again from a
	// union and the record that refers to itself.
	for _, name := range []string{"orders.v1.OrderStatus", "orders.v1.Customer", "common.v1.Address", "orders.v1.LineItem", "orders.v1.Discount", "orders.v1.Category"} {
		if _, ok := defs[name]; !ok {
			t.Errorf("$defs lacks %q; holds %v", name, keys(defs))
		}
	}

	// Exactly the fields without an Avro default are required.
	required := got["required"].([]any)
	want := []any{"orderId", "placedAt", "status", "customer", "lines", "metadata", "category", "priority", "total"}
	if !slices.Equal(required, want) {
		t.Errorf("required = %v, want %v", required, want)
	}

	properties := got["properties"].(map[string]any)
	property := func(name string) map[string]any { return properties[name].(map[string]any) }

	// A union whose non-null branch is a referenced enum, a fullname reference
	// across namespaces, and an array.
	for name, branch := range map[string]string{
		"previousStatus":  "#/$defs/orders.v1.OrderStatus",
		"shippingAddress": "#/$defs/common.v1.Address",
	} {
		anyOf := property(name)["anyOf"].([]any)
		if anyOf[0].(map[string]any)["type"] != "null" || anyOf[1].(map[string]any)["$ref"] != branch {
			t.Errorf("%s = %v, want null or %s", name, anyOf, branch)
		}
	}
	if notes := property("notes")["anyOf"].([]any)[1].(map[string]any); notes["type"] != "array" {
		t.Errorf("notes non-null branch = %v, want an array", notes)
	}

	// Map values: a nullable union, and base64 bytes.
	if values := property("metadata")["additionalProperties"].(map[string]any); len(values["anyOf"].([]any)) != 2 {
		t.Errorf("metadata values = %v, want a nullable union", values)
	}
	if values := property("attachments")["additionalProperties"].(map[string]any); values["contentEncoding"] != "base64" {
		t.Errorf("attachments values = %v, want base64 strings", values)
	}

	// Records nested at any depth stay closed, and int bounds reach inside
	// array items.
	for _, name := range []string{"orders.v1.Customer", "common.v1.Address", "orders.v1.LineItem", "orders.v1.Discount", "orders.v1.Category"} {
		if defs[name].(map[string]any)["additionalProperties"] != false {
			t.Errorf("%s does not reject unknown fields", name)
		}
	}
	quantity := defs["orders.v1.LineItem"].(map[string]any)["properties"].(map[string]any)["quantity"].(map[string]any)
	if quantity["maximum"] != float64(2147483647) {
		t.Errorf("lines[].quantity = %v, want int32 bounds", quantity)
	}
}
