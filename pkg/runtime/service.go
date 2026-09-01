// Package runtime contains the SDK-neutral producer implementation used by generated tools.
package runtime

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/twmb/avro"
)

type Tool struct {
	Name     string
	Topic    string
	Subject  string
	KeyField string
	Schema   []byte
}

type Event struct {
	Topic    string
	Key      []byte
	Value    []byte
	SchemaID int
}

type PublishResult struct {
	Topic     string    `json:"topic"`
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	SchemaID  int       `json:"schemaId"`
	Timestamp time.Time `json:"timestamp"`
}

type SchemaResolver interface {
	Resolve(ctx context.Context, subject string, localSchema []byte) (int, error)
}

type Publisher interface {
	Publish(ctx context.Context, event Event) (PublishResult, error)
}

type Service struct {
	resolver        SchemaResolver
	publisher       Publisher
	maxMessageBytes int
}

type ServiceOption func(*Service)

func WithMaxMessageBytes(limit int) ServiceOption {
	return func(s *Service) {
		if limit > 0 {
			s.maxMessageBytes = limit
		}
	}
}

func NewService(resolver SchemaResolver, publisher Publisher, options ...ServiceOption) *Service {
	service := &Service{resolver: resolver, publisher: publisher, maxMessageBytes: 1 << 20}
	for _, option := range options {
		option(service)
	}
	return service
}

func (s *Service) Publish(ctx context.Context, tool Tool, payload map[string]any) (PublishResult, error) {
	if tool.Topic == "" || tool.Subject == "" {
		return PublishResult{}, fmt.Errorf("tool topic and subject are required")
	}
	schema, err := avro.Parse(string(tool.Schema))
	if err != nil {
		return PublishResult{}, fmt.Errorf("parse embedded schema: %w", err)
	}
	schemaID, err := s.resolver.Resolve(ctx, tool.Subject, tool.Schema)
	if err != nil {
		return PublishResult{}, fmt.Errorf("resolve schema subject %q: %w", tool.Subject, err)
	}
	key, err := keyFor(tool.KeyField, payload)
	if err != nil {
		return PublishResult{}, err
	}
	encoded, err := schema.Encode(payload)
	if err != nil {
		return PublishResult{}, fmt.Errorf("encode Avro payload: %w", err)
	}
	if len(encoded) > s.maxMessageBytes {
		return PublishResult{}, fmt.Errorf("encoded Avro payload exceeds %d byte limit", s.maxMessageBytes)
	}
	value := make([]byte, 5+len(encoded))
	binary.BigEndian.PutUint32(value[1:5], uint32(schemaID))
	copy(value[5:], encoded)
	result, err := s.publisher.Publish(ctx, Event{Topic: tool.Topic, Key: key, Value: value, SchemaID: schemaID})
	if err != nil {
		return PublishResult{}, fmt.Errorf("publish %q: %w", tool.Topic, err)
	}
	return result, nil
}

func keyFor(field string, payload map[string]any) ([]byte, error) {
	if field == "" {
		return nil, nil
	}
	value, ok := payload[field]
	if !ok {
		return nil, fmt.Errorf("key field %q is required", field)
	}
	key, ok := value.(string)
	if !ok || key == "" {
		return nil, fmt.Errorf("key field %q must be a non-empty string", field)
	}
	return []byte(key), nil
}
