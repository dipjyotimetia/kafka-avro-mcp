package runtime

import (
	"context"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/sr"
)

// RegistryResolver performs only schema lookup; it never registers schemas.
type RegistryResolver struct {
	Client *sr.Client
	mu     sync.Mutex
	ids    map[string]int
}

func (r *RegistryResolver) Resolve(ctx context.Context, subject string, local []byte) (int, error) {
	if r.Client == nil {
		return 0, fmt.Errorf("Schema Registry client is required")
	}
	key := subject + "\x00" + string(local)
	r.mu.Lock()
	if id, ok := r.ids[key]; ok {
		r.mu.Unlock()
		return id, nil
	}
	r.mu.Unlock()
	found, err := r.Client.LookupSchema(ctx, subject, sr.Schema{Schema: string(local), Type: sr.TypeAvro})
	if err != nil {
		return 0, fmt.Errorf("exact schema is not registered: %w", err)
	}
	r.mu.Lock()
	if r.ids == nil {
		r.ids = map[string]int{}
	}
	r.ids[key] = found.ID
	r.mu.Unlock()
	return found.ID, nil
}
