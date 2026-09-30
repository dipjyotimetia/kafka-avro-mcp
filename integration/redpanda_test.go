// Package integration exercises the runtime against a real broker and Schema
// Registry. It is its own module so that testcontainers and its Docker
// dependencies stay out of the library's go.mod.
//
// Run with: cd integration && go test ./...
// Requires a working Docker daemon.

package integration

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/jsonschema"
	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/runtime"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/avro"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sr"
)

// cluster is one Redpanda container shared by every subtest: starting it is
// by far the slowest part of the run.
type cluster struct {
	broker   string
	registry *sr.Client
	producer *kgo.Client
}

func TestPublishAvroRecordThroughRedpanda(t *testing.T) {
	// Docker Desktop's port-forwarding can prevent Ryuk from becoming ready;
	// this test always terminates its own container.
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	c := startCluster(ctx, t)

	t.Run("simple order", func(t *testing.T) {
		schema := []byte(`{"type":"record","name":"OrderCreated","fields":[{"name":"orderId","type":"string"}]}`)
		id := c.prepare(ctx, t, "orders.created", "orders.created-value", schema)

		service := runtime.NewService(&runtime.RegistryResolver{Client: c.registry}, runtime.KafkaPublisher{Client: c.producer})
		result, err := service.Publish(ctx, runtime.Tool{Topic: "orders.created", Subject: "orders.created-value", KeyField: "orderId", Schema: schema}, map[string]any{"orderId": "o-1"})
		if err != nil {
			t.Fatal(err)
		}
		if result.SchemaID != id {
			t.Fatalf("schema ID = %d, want %d", result.SchemaID, id)
		}
		c.consumeOne(ctx, t, "orders.created", "o-1", id)
	})

	// The example's complex event, sent through the MCP tool path: the
	// registry's exact-schema lookup has to match a schema with named types,
	// two namespaces and recursion, and base64 and nested unions have to
	// survive to the broker.
	t.Run("complex order placed", func(t *testing.T) {
		schema, err := os.ReadFile("../examples/orders/order-placed.avsc")
		if err != nil {
			t.Fatal(err)
		}
		id := c.prepare(ctx, t, "orders.placed", "orders.placed-value", schema)

		input, err := jsonschema.Convert(schema)
		if err != nil {
			t.Fatal(err)
		}
		if input, err = jsonschema.MarkKey(input, "orderId"); err != nil {
			t.Fatal(err)
		}
		server := &serverStub{}
		service := runtime.NewService(&runtime.RegistryResolver{Client: c.registry}, runtime.KafkaPublisher{Client: c.producer})
		runtime.RegisterTool(server, service,
			runtime.Tool{Name: "publish_order_placed", Topic: "orders.placed", Subject: "orders.placed-value", KeyField: "orderId", Schema: schema},
			json.RawMessage(input), "Publish an order.")
		result := server.handler(ctx, runtime.CallToolRequest{Arguments: json.RawMessage(`{
			"orderId": "o-2",
			"placedAt": 9007199254740993,
			"status": "CONFIRMED",
			"previousStatus": "PENDING",
			"customer": {"id": "c-1", "address": {"line1": "1 High St", "city": "London", "postcode": "N1 1AA"}},
			"shippingAddress": {"line1": "2 Low Rd", "city": "Leeds", "postcode": "LS1 1AA"},
			"lines": [{"sku": "s-1", "quantity": 2, "unitPriceMinor": 1250, "discount": {"code": "TEN", "amountMinor": 125}}],
			"metadata": {"channel": "web", "campaign": null},
			"attachments": {"receipt": "aGVsbG8="},
			"category": {"name": "shirts", "parent": {"name": "clothing", "parent": null}},
			"priority": 1.5,
			"total": 23.75
		}`)})
		if result.IsError {
			t.Fatalf("tool result = %s", result.Error)
		}

		value := c.consumeOne(ctx, t, "orders.placed", "o-2", id)
		parsed, err := avro.Parse(string(schema))
		if err != nil {
			t.Fatal(err)
		}
		var decoded map[string]any
		if _, err := parsed.Decode(value[5:], &decoded); err != nil {
			t.Fatalf("decoding the consumed record: %v", err)
		}
		if decoded["placedAt"] != int64(9007199254740993) {
			t.Errorf("placedAt = %#v, want 2^53+1 exactly", decoded["placedAt"])
		}
		if receipt := decoded["attachments"].(map[string]any)["receipt"]; string(receipt.([]byte)) != "hello" {
			t.Errorf("attachments.receipt = %#v, want the decoded bytes", receipt)
		}
		if parent := decoded["category"].(map[string]any)["parent"].(map[string]any); parent["name"] != "clothing" {
			t.Errorf("category.parent = %#v", parent)
		}
		if discount := decoded["lines"].([]any)[0].(map[string]any)["discount"].(map[string]any); discount["code"] != "TEN" {
			t.Errorf("lines[0].discount = %#v", discount)
		}
	})
}

type serverStub struct{ handler runtime.ToolHandler }

func (s *serverStub) AddTool(_ runtime.ToolDefinition, handler runtime.ToolHandler) {
	s.handler = handler
}

func startCluster(ctx context.Context, t *testing.T) *cluster {
	t.Helper()
	rp, err := redpanda.Run(ctx, "docker.redpanda.com/redpandadata/redpanda:v23.3.3", redpanda.WithAutoCreateTopics(), testcontainers.WithExposedPorts("9092/tcp", "9644/tcp", "8081/tcp", "8082/tcp"))
	// Schedule teardown before the error check and independently of ctx: when the
	// test exhausts its budget, ctx is already cancelled and Terminate(ctx) fails,
	// orphaning the container (Ryuk is disabled above).
	testcontainers.CleanupContainer(t, rp)
	if err != nil {
		t.Fatal(err)
	}
	broker, err := rp.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatal(err)
	}
	registryURL, err := rp.SchemaRegistryAddress(ctx)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := sr.NewClient(sr.URLs(registryURL))
	if err != nil {
		t.Fatal(err)
	}
	producer, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	return &cluster{broker: broker, registry: registry, producer: producer}
}

// prepare registers schema under subject and creates topic, returning the
// registered schema ID.
func (c *cluster) prepare(ctx context.Context, t *testing.T, topic, subject string, schema []byte) int {
	t.Helper()
	registered, err := c.registry.CreateSchema(ctx, subject, sr.Schema{Schema: string(schema), Type: sr.TypeAvro})
	if err != nil {
		t.Fatal(err)
	}

	// Create the topic via the Kafka admin request API (kmsg). This mirrors franz-go's test helpers
	// and avoids relying on auto-create settings.
	req := kmsg.NewPtrCreateTopicsRequest()
	reqTopic := kmsg.NewCreateTopicsRequestTopic()
	reqTopic.Topic = topic
	reqTopic.NumPartitions = 1
	reqTopic.ReplicationFactor = 1
	req.Topics = append(req.Topics, reqTopic)

	// Retry on dial errors briefly (same pattern franz-go tests use is acceptable here).
	start := time.Now()
	for {
		resp, err := req.RequestWith(ctx, c.producer)
		if ne := (*net.OpError)(nil); errors.As(err, &ne) && ne.Op == "dial" && time.Since(start) < 30*time.Second {
			time.Sleep(time.Second)
			continue
		}
		if err == nil {
			// Translate server error code
			if len(resp.Topics) == 0 {
				t.Fatalf("CreateTopics returned no topic results for %q", topic)
			}
			err = kerr.ErrorForCode(resp.Topics[0].ErrorCode)
			if errors.Is(err, kerr.TopicAlreadyExists) {
				err = nil
			}
		}
		if err != nil {
			t.Fatalf("unable to create topic %q: %v", topic, err)
		}
		return registered.ID
	}
}

// consumeOne reads the first record on topic, checks its key and Confluent
// wire header, and returns its value.
func (c *cluster) consumeOne(ctx context.Context, t *testing.T, topic, key string, schemaID int) []byte {
	t.Helper()
	consumer, err := kgo.NewClient(kgo.SeedBrokers(c.broker), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	for {
		fetches := consumer.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatal(err)
		}
		if fetches.NumRecords() == 0 {
			continue
		}
		record := fetches.Records()[0]
		if record.Topic != topic || string(record.Key) != key {
			t.Fatalf("record = %#v", record)
		}
		if len(record.Value) < 5 {
			t.Fatalf("record value = %v, want at least a 5-byte wire header", record.Value)
		}
		if record.Value[0] != 0 || int(binary.BigEndian.Uint32(record.Value[1:5])) != schemaID {
			t.Fatalf("wire header = %v", record.Value[:5])
		}
		return record.Value
	}
}
