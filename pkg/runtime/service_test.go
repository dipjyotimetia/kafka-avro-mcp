package runtime

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type resolverStub struct{ id int }

func (r resolverStub) Resolve(context.Context, string, []byte) (int, error) { return r.id, nil }

type publisherStub struct{ event Event }

func (p *publisherStub) Publish(_ context.Context, event Event) (PublishResult, error) {
	p.event = event
	return PublishResult{Topic: event.Topic, Partition: 2, Offset: 7, SchemaID: event.SchemaID}, nil
}

func TestServicePublishesFixedTopicWithConfluentHeaderAndDerivedKey(t *testing.T) {
	publisher := &publisherStub{}
	service := NewService(resolverStub{id: 42}, publisher)
	result, err := service.Publish(context.Background(), Tool{
		Topic: "orders.created", Subject: "orders.created-value", KeyField: "orderId",
		Schema: []byte(`{"type":"record","name":"OrderCreated","fields":[{"name":"orderId","type":"string"},{"name":"amount","type":"double"}]}`),
	}, map[string]any{"orderId": "o-1", "amount": 12.5})
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if result.SchemaID != 42 || publisher.event.Topic != "orders.created" || string(publisher.event.Key) != "o-1" {
		t.Fatalf("result/event = %#v / %#v", result, publisher.event)
	}
	if publisher.event.Value[0] != 0 || binary.BigEndian.Uint32(publisher.event.Value[1:5]) != 42 {
		t.Fatalf("wire header = %v", publisher.event.Value[:5])
	}
}

func TestServiceRejectsNonStringConfiguredKey(t *testing.T) {
	service := NewService(resolverStub{id: 1}, &publisherStub{})
	_, err := service.Publish(context.Background(), Tool{Topic: "orders.created", Subject: "orders.created-value", KeyField: "orderId", Schema: []byte(`{"type":"record","name":"OrderCreated","fields":[{"name":"orderId","type":"string"}]}`)}, map[string]any{"orderId": 1})
	if err == nil {
		t.Fatal("Publish() accepted a non-string configured key")
	}
}

func TestServiceRejectsEncodedPayloadOverConfiguredLimit(t *testing.T) {
	service := NewService(resolverStub{id: 1}, &publisherStub{}, WithMaxMessageBytes(1))
	_, err := service.Publish(context.Background(), Tool{Topic: "orders.created", Subject: "orders.created-value", Schema: []byte(`{"type":"record","name":"OrderCreated","fields":[{"name":"value","type":"string"}]}`)}, map[string]any{"value": "too large"})
	if err == nil {
		t.Fatal("Publish() accepted payload exceeding configured limit")
	}
}

// The limit applies to the produced record, not the bare Avro payload: a
// payload that fits exactly still overflows once the 5-byte wire header and
// the key are added.
func TestServiceCountsWireHeaderAndKeyAgainstLimit(t *testing.T) {
	// {"id":"k"} encodes to 2 Avro bytes (zigzag length + 'k'), the key is 1
	// byte, and the Confluent wire header is 5, so the record is exactly 8.
	publish := func(limit int) error {
		service := NewService(resolverStub{id: 1}, &publisherStub{}, WithMaxMessageBytes(limit))
		_, err := service.Publish(context.Background(), Tool{
			Topic: "orders.created", Subject: "orders.created-value", KeyField: "id",
			Schema: []byte(`{"type":"record","name":"E","fields":[{"name":"id","type":"string"}]}`),
		}, map[string]any{"id": "k"})
		return err
	}
	if err := publish(8); err != nil {
		t.Fatalf("Publish() rejected a record exactly at the limit: %v", err)
	}
	if err := publish(7); err == nil {
		t.Fatal("Publish() accepted a 8-byte record under a 7-byte limit")
	}
	// The payload alone is 2 bytes; only counting it would let this through.
	if err := publish(2); err == nil {
		t.Fatal("Publish() sized the Avro payload instead of the produced record")
	}
}

type failingResolver struct{}

func (failingResolver) Resolve(context.Context, string, []byte) (int, error) {
	return 0, errors.New("registry unreachable")
}

// A payload the caller can fix must be reported as such even while the
// registry is down, rather than being masked by the infrastructure failure.
func TestServiceReportsPayloadErrorsBeforeResolvingSchema(t *testing.T) {
	service := NewService(failingResolver{}, &publisherStub{}, WithMaxMessageBytes(1))
	_, err := service.Publish(context.Background(), Tool{Topic: "orders.created", Subject: "orders.created-value", Schema: []byte(`{"type":"record","name":"E","fields":[{"name":"value","type":"string"}]}`)}, map[string]any{"value": "too large"})
	var payload PayloadError
	if !errors.As(err, &payload) {
		t.Fatalf("Publish() error = %v, want a PayloadError", err)
	}
}

type blockingPublisher struct{}

func (blockingPublisher) Publish(ctx context.Context, _ Event) (PublishResult, error) {
	<-ctx.Done()
	return PublishResult{}, ctx.Err()
}

func TestServiceBoundsPublishWithTimeout(t *testing.T) {
	service := NewService(resolverStub{id: 1}, blockingPublisher{}, WithPublishTimeout(20*time.Millisecond))
	done := make(chan error, 1)
	go func() {
		_, err := service.Publish(context.Background(), Tool{Topic: "orders.created", Subject: "orders.created-value", Schema: []byte(`{"type":"record","name":"E","fields":[{"name":"id","type":"string"}]}`)}, map[string]any{"id": "k"})
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Publish() error = %v, want deadline exceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Publish() did not honour the publish timeout")
	}
}

// A publish is a side effect an LLM chose to cause, so each one leaves a
// record of where it landed.
func TestServiceLogsEverySuccessfulPublish(t *testing.T) {
	var log bytes.Buffer
	service := NewService(resolverStub{id: 3}, &publisherStub{}, WithLogger(slog.New(slog.NewTextHandler(&log, nil))))
	_, err := service.Publish(context.Background(), Tool{Name: "publish_order", Topic: "orders.created", Subject: "orders.created-value", Schema: []byte(`{"type":"record","name":"E","fields":[{"name":"id","type":"string"}]}`)}, map[string]any{"id": "k"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"level=INFO", "tool=publish_order", "topic=orders.created", "partition=2", "offset=7", "schemaId=3"} {
		if !strings.Contains(log.String(), want) {
			t.Errorf("log %q does not contain %q", log.String(), want)
		}
	}
}
