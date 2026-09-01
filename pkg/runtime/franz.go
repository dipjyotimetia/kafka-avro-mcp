package runtime

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaPublisher struct{ Client *kgo.Client }

func (p KafkaPublisher) Publish(ctx context.Context, event Event) (PublishResult, error) {
	if p.Client == nil {
		return PublishResult{}, fmt.Errorf("Kafka client is required")
	}
	record, err := p.Client.ProduceSync(ctx, &kgo.Record{Topic: event.Topic, Key: event.Key, Value: event.Value}).First()
	if err != nil {
		return PublishResult{}, err
	}
	return PublishResult{Topic: record.Topic, Partition: record.Partition, Offset: record.Offset, SchemaID: event.SchemaID, Timestamp: record.Timestamp}, nil
}
