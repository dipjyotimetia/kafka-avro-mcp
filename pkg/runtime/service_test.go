package runtime

import (
	"context"
	"encoding/binary"
	"testing"
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
