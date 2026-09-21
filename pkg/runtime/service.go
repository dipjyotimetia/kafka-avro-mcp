// Package runtime contains the SDK-neutral producer implementation used by generated tools.
package runtime

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/avro"
)

// PayloadError marks a failure the caller can fix by sending different
// arguments, as opposed to a broker or registry problem it can do nothing
// about. RegisterTool relays these to the model verbatim and keeps the rest
// in the server's logs.
type PayloadError struct{ Err error }

func (e PayloadError) Error() string { return e.Err.Error() }
func (e PayloadError) Unwrap() error { return e.Err }

func payloadErrorf(format string, args ...any) error {
	return PayloadError{Err: fmt.Errorf(format, args...)}
}

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
	logger          *slog.Logger
}

type ServiceOption func(*Service)

func WithMaxMessageBytes(limit int) ServiceOption {
	return func(s *Service) {
		if limit > 0 {
			s.maxMessageBytes = limit
		}
	}
}

// WithLogger directs the detail of infrastructure failures somewhere other
// than the default logger. That detail never reaches the model, so this is the
// only place a broker or registry error is recorded in full.
func WithLogger(logger *slog.Logger) ServiceOption {
	return func(s *Service) {
		if logger != nil {
			s.logger = logger
		}
	}
}

func NewService(resolver SchemaResolver, publisher Publisher, options ...ServiceOption) *Service {
	service := &Service{resolver: resolver, publisher: publisher, maxMessageBytes: 1 << 20, logger: slog.Default()}
	for _, option := range options {
		option(service)
	}
	return service
}

// Publish parses the tool's embedded schema on every call. Registered tools go
// through publish instead, which takes a schema parsed once at registration.
func (s *Service) Publish(ctx context.Context, tool Tool, payload map[string]any) (PublishResult, error) {
	schema, err := avro.Parse(string(tool.Schema))
	if err != nil {
		return PublishResult{}, fmt.Errorf("parse embedded schema: %w", err)
	}
	return s.publish(ctx, tool, schema, payload)
}

func (s *Service) publish(ctx context.Context, tool Tool, schema *avro.Schema, payload map[string]any) (PublishResult, error) {
	if tool.Topic == "" || tool.Subject == "" {
		return PublishResult{}, fmt.Errorf("tool topic and subject are required")
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
		return PublishResult{}, payloadErrorf("encode Avro payload: %w", err)
	}
	// Count the wire-format header and the key: the broker sizes the whole
	// record, not the Avro payload alone. Kafka also charges per-record batch
	// overhead, so this remains a lower bound on what the broker sees.
	if size := 5 + len(encoded) + len(key); size > s.maxMessageBytes {
		return PublishResult{}, payloadErrorf("record of %d bytes exceeds %d byte limit", size, s.maxMessageBytes)
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
		return nil, payloadErrorf("key field %q is required", field)
	}
	key, ok := value.(string)
	if !ok || key == "" {
		return nil, payloadErrorf("key field %q must be a non-empty string", field)
	}
	return []byte(key), nil
}
