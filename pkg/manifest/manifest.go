// Package manifest loads the explicit Kafka-to-MCP overlay used by the generator.
package manifest

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

const APIVersion = "mcp.kafka/v1alpha1"

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
	if len(config.Events) == 0 {
		return nil, fmt.Errorf("at least one event is required")
	}

	names := make(map[string]struct{}, len(config.Events))
	tools := make(map[string]struct{}, len(config.Events))
	for i, event := range config.Events {
		at := fmt.Sprintf("events[%d]", i)
		if strings.TrimSpace(event.Name) == "" || strings.TrimSpace(event.Schema) == "" {
			return nil, fmt.Errorf("%s name and schema are required", at)
		}
		if strings.TrimSpace(event.Kafka.Topic) == "" || strings.TrimSpace(event.Kafka.Subject) == "" {
			return nil, fmt.Errorf("%s kafka.topic and kafka.subject are required", at)
		}
		if strings.TrimSpace(event.MCP.Tool) == "" {
			return nil, fmt.Errorf("%s mcp.tool is required", at)
		}
		if _, ok := names[event.Name]; ok {
			return nil, fmt.Errorf("duplicate event name %q", event.Name)
		}
		if _, ok := tools[event.MCP.Tool]; ok {
			return nil, fmt.Errorf("duplicate MCP tool %q", event.MCP.Tool)
		}
		names[event.Name] = struct{}{}
		tools[event.MCP.Tool] = struct{}{}
	}
	return &config, nil
}
