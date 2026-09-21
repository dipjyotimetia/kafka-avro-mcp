// Package manifest loads the explicit Kafka-to-MCP overlay used by the generator.
package manifest

import (
	"fmt"
	"go/token"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

const APIVersion = "mcp.kafka/v1alpha1"

var toolName = regexp.MustCompile(`^[a-z_][a-z0-9_-]{0,63}$`)

// kafkaName is Kafka's own legal topic charset. Topics are fixed at generation
// time, so a typo here would otherwise only surface as a broker error on the
// first publish.
var kafkaName = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)

type Config struct {
	APIVersion string  `yaml:"apiVersion"`
	Package    string  `yaml:"package"`
	Events     []Event `yaml:"events"`
}

type Event struct {
	Name   string `yaml:"name"`
	Schema string `yaml:"schema"`
	Kafka  Kafka  `yaml:"kafka"`
	MCP    MCP    `yaml:"mcp"`
}

type Kafka struct {
	Topic   string `yaml:"topic"`
	Subject string `yaml:"subject"`
	Key     Key    `yaml:"key"`
}

type Key struct {
	Field string `yaml:"field"`
}

type MCP struct {
	Tool        string `yaml:"tool"`
	Description string `yaml:"description"`
}

func Load(data []byte) (*Config, error) {
	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	if config.APIVersion != APIVersion {
		return nil, fmt.Errorf("apiVersion must be %q", APIVersion)
	}
	if strings.TrimSpace(config.Package) == "" {
		return nil, fmt.Errorf("package is required")
	}
	// The package name is interpolated straight into the generated file, so
	// anything that is not an identifier becomes a confusing gofmt error.
	if !token.IsIdentifier(config.Package) || token.Lookup(config.Package).IsKeyword() {
		return nil, fmt.Errorf("package %q is not a valid Go package name", config.Package)
	}
	if len(config.Events) == 0 {
		return nil, fmt.Errorf("at least one event is required")
	}

	names := make(map[string]struct{}, len(config.Events))
	tools := make(map[string]struct{}, len(config.Events))
	identifiers := make(map[string]string, len(config.Events))
	for i, event := range config.Events {
		at := fmt.Sprintf("events[%d]", i)
		if strings.TrimSpace(event.Name) == "" || strings.TrimSpace(event.Schema) == "" {
			return nil, fmt.Errorf("%s name and schema are required", at)
		}
		if strings.TrimSpace(event.Kafka.Topic) == "" || strings.TrimSpace(event.Kafka.Subject) == "" {
			return nil, fmt.Errorf("%s kafka.topic and kafka.subject are required", at)
		}
		if !kafkaName.MatchString(event.Kafka.Topic) || event.Kafka.Topic == "." || event.Kafka.Topic == ".." {
			return nil, fmt.Errorf("%s kafka.topic %q is not a legal Kafka topic name", at, event.Kafka.Topic)
		}
		// The subject is placed in a Schema Registry URL path.
		if strings.ContainsAny(event.Kafka.Subject, " \t\n/?#") {
			return nil, fmt.Errorf("%s kafka.subject %q contains characters that are not safe in a registry URL", at, event.Kafka.Subject)
		}
		if strings.TrimSpace(event.MCP.Tool) == "" {
			return nil, fmt.Errorf("%s mcp.tool is required", at)
		}
		if !toolName.MatchString(event.MCP.Tool) {
			return nil, fmt.Errorf("%s mcp.tool %q must match %s", at, event.MCP.Tool, toolName)
		}
		if _, ok := names[event.Name]; ok {
			return nil, fmt.Errorf("duplicate event name %q", event.Name)
		}
		if _, ok := tools[event.MCP.Tool]; ok {
			return nil, fmt.Errorf("duplicate MCP tool %q", event.MCP.Tool)
		}
		// Distinct tool names can still collapse to the same generated Go
		// identifier (publish_order and publish-order both yield PublishOrder),
		// which would emit duplicate declarations in the generated package.
		identifier := Pascal(event.MCP.Tool)
		if previous, ok := identifiers[identifier]; ok {
			return nil, fmt.Errorf("MCP tools %q and %q both map to generated identifier %q", previous, event.MCP.Tool, identifier)
		}
		names[event.Name] = struct{}{}
		tools[event.MCP.Tool] = struct{}{}
		identifiers[identifier] = event.MCP.Tool
	}
	return &config, nil
}

// Pascal converts an MCP tool name into the Go identifier prefix used for its
// generated declarations. Both the generator and the manifest's uniqueness
// check depend on it, so it lives here rather than in the generator.
func Pascal(value string) string {
	parts := strings.FieldsFunc(value, func(r rune) bool { return r == '_' || r == '-' || r == '.' })
	for i := range parts {
		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
	}
	return strings.Join(parts, "")
}
