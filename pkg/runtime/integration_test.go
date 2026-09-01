package runtime_test

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/dipjyotimetia/kafka-avro-mcp/pkg/runtime"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
)

func TestPublishAvroRecordThroughRedpanda(t *testing.T) {
	// Docker Desktop's port-forwarding can prevent Ryuk from becoming ready;
	// this test always terminates its own container.
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	rp, err := redpanda.Run(ctx, "docker.redpanda.com/redpandadata/redpanda:v23.3.3", redpanda.WithAutoCreateTopics(), testcontainers.WithExposedPorts("9092/tcp", "9644/tcp", "8081/tcp", "8082/tcp"))
	if err != nil { t.Fatal(err) }
	defer rp.Terminate(ctx)
	broker, err := rp.KafkaSeedBroker(ctx); if err != nil { t.Fatal(err) }
	registryURL, err := rp.SchemaRegistryAddress(ctx); if err != nil { t.Fatal(err) }
	registry, err := sr.NewClient(sr.URLs(registryURL)); if err != nil { t.Fatal(err) }
	schema := []byte(`{"type":"record","name":"OrderCreated","fields":[{"name":"orderId","type":"string"}]}`)
	registered, err := registry.CreateSchema(ctx, "orders.created-value", sr.Schema{Schema: string(schema), Type: sr.TypeAvro})
	if err != nil { t.Fatal(err) }

	consumer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics("orders.created"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil { t.Fatal(err) }
	defer consumer.Close()
	producer, err := kgo.NewClient(kgo.SeedBrokers(broker)); if err != nil { t.Fatal(err) }
	defer producer.Close()
	service := runtime.NewService(&runtime.RegistryResolver{Client: registry}, runtime.KafkaPublisher{Client: producer})
	result, err := service.Publish(ctx, runtime.Tool{Topic: "orders.created", Subject: "orders.created-value", KeyField: "orderId", Schema: schema}, map[string]any{"orderId":"o-1"})
	if err != nil { t.Fatal(err) }
	if result.SchemaID != registered.ID { t.Fatalf("schema ID = %d, want %d", result.SchemaID, registered.ID) }
	for {
		fetches := consumer.PollFetches(ctx)
		if err := fetches.Err(); err != nil { t.Fatal(err) }
		if fetches.NumRecords() == 0 { continue }
		record := fetches.Records()[0]
		if record.Topic != "orders.created" || string(record.Key) != "o-1" { t.Fatalf("record = %#v", record) }
		if record.Value[0] != 0 || int(binary.BigEndian.Uint32(record.Value[1:5])) != registered.ID { t.Fatalf("wire header = %v", record.Value[:5]) }
		return
	}
}
