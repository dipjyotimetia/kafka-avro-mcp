# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`kafka-avro-mcp` compiles an Avro event contract plus a YAML Kafka/MCP overlay (`kafka.mcp.yaml`) into static Go code that registers fixed-topic MCP "publish" tools. There is no dynamic discovery: topics, subjects, key fields and schemas are baked into generated constants.

## Commands

```bash
go build ./...                      # also builds examples/orders/server
go test -race ./...                 # what CI runs
go test ./pkg/runtime -run TestName # single test
go vet ./...
gofmt -l ./cmd ./pkg ./examples ./integration   # CI fails on any output
golangci-lint run                   # CI uses v2.13.2, default config (no .golangci.yml)

# Integration tests (Redpanda via testcontainers; needs Docker). Separate Go module.
cd integration && go test ./...

# CLI
go run ./cmd/avro-gen-go-mcp validate --config examples/orders/kafka.mcp.yaml [--registry-url URL --registry-user U]
go run ./cmd/avro-gen-go-mcp generate --config examples/orders/kafka.mcp.yaml --out examples/orders/gen

# Release: push a v* tag; .github/workflows/release.yml runs GoReleaser
# (binaries + Homebrew cask in dipjyotimetia/homebrew-tap). Local dry run:
goreleaser release --snapshot --clean --skip=publish
```

CI regenerates `examples/orders/gen` and runs `git diff --exit-code`. Any change to the generator, JSON Schema converter or the example schemas must be followed by regenerating and committing `examples/orders/gen/tools.mcp.go` — never hand-edit it.

`integration/` has its own `go.mod` so testcontainers stays out of the library's dependency graph; lint/vet/test it separately (golangci-lint stops at the nested module).

## Architecture

Pipeline: manifest -> Avro schema -> JSON Schema -> generated Go -> runtime.

- `pkg/manifest` — loads/validates the overlay (`apiVersion: mcp.kafka/v1alpha1`, `package`, `events[]` with `schema`, `kafka.{topic,subject,key.field}`, `mcp.{tool,description}`). Schema paths are relative to the manifest file.
- `pkg/jsonschema` — converts the supported Avro subset to JSON Schema 2020-12 (`Convert`), checks the key field (`ValidateKey`) and marks it `minLength: 1` (`MarkKey`). Named types become `$defs` keyed by Avro fullname. `spec_test.go` files walk the Avro 1.10–1.12 spec rule-by-rule.
- `pkg/generator` — emits one file per manifest: `<Pascal>Tool` (`runtime.Tool` with embedded Avro schema), `<Pascal>InputSchema`, and a single `RegisterTools(server runtime.MCPServer, service *runtime.Service)`.
- `pkg/validate` — local validation plus a read-only Schema Registry check (never registers).
- `pkg/runtime` — SDK-neutral core used by generated code:
  - `RegisterTool` (`mcp.go`) parses the Avro schema once at registration (panics on bad generated constants) and enforces the input schema itself in `payload.go` — neither MCP SDK reliably validates. It also base64-decodes `bytes` fields and rejects unknown fields.
  - `Service.publish` (`service.go`): key extraction -> Avro encode -> size check -> registry lookup (`SchemaResolver`, lookup only, no registration) -> Confluent wire format (magic byte + 4-byte schema ID) -> `Publisher`. Payload checks deliberately run before the registry lookup.
  - Errors: `PayloadError` is returned verbatim to the model; any other error is logged via `slog` and the tool result only names the topic (avoids leaking broker/registry hostnames). Preserve this split when adding failure paths.
  - Options: `WithMaxMessageBytes` (default 1 MiB), `WithPublishTimeout` (default 30s), `WithLogger`.
  - `KafkaPublisher` (franz-go) and `RegistryResolver` / `NewRegistryClient` are the concrete implementations.
- `pkg/runtime/gosdk` and `pkg/runtime/mcpgo` — thin `Wrap` adapters for `modelcontextprotocol/go-sdk` and `mark3labs/mcp-go`, kept in separate packages so users compile only the SDK they use.
- `examples/orders/server` — end-to-end stdio server wiring; env vars `KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL`, `SCHEMA_REGISTRY_USER`, `SCHEMA_REGISTRY_PASSWORD`, optional `KAFKA_TLS`, `KAFKA_SASL_MECHANISM`/`_USER`/`_PASSWORD`.

## Deliberate constraints (V1)

Supported: record roots, primitives, nested records, arrays, maps, enums, nullable unions. Intentionally unsupported: caller-controlled topics, schema registration, headers, consumers, `fixed`, spec-defined logical types, non-nullable multi-branch unions. Unknown/misplaced `logicalType` is ignored per the Avro spec. A field is optional in JSON Schema only if it has an Avro default (nullable alone is not enough); `int`/`long` advertise their 32/64-bit bounds.

Registry passwords are read only from `$SCHEMA_REGISTRY_PASSWORD`, never from a flag.
