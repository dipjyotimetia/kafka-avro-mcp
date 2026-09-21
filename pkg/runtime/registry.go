package runtime

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/twmb/franz-go/pkg/sr"
)

// NewRegistryClient builds a Schema Registry client, falling back to
// SCHEMA_REGISTRY_URL and SCHEMA_REGISTRY_USER for anything not passed. The
// password comes only from SCHEMA_REGISTRY_PASSWORD: arguments are visible to
// anything that can list processes, which is not where a credential belongs.
//
// It returns a nil client when no URL is configured, which callers treat as
// "no registry gate" rather than an error.
func NewRegistryClient(url, user string) (*sr.Client, error) {
	if url == "" {
		url = os.Getenv("SCHEMA_REGISTRY_URL")
	}
	if url == "" {
		return nil, nil
	}
	if user == "" {
		user = os.Getenv("SCHEMA_REGISTRY_USER")
	}
	options := []sr.ClientOpt{sr.URLs(url)}
	if password := os.Getenv("SCHEMA_REGISTRY_PASSWORD"); user != "" || password != "" {
		options = append(options, sr.BasicAuth(user, password))
	}
	return sr.NewClient(options...)
}

// RegistryResolver performs only schema lookup; it never registers schemas.
type RegistryResolver struct {
	Client *sr.Client
	mu     sync.Mutex
	ids    map[string]int
}

func (r *RegistryResolver) Resolve(ctx context.Context, subject string, local []byte) (int, error) {
	if r.Client == nil {
		return 0, fmt.Errorf("no Schema Registry client configured")
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
