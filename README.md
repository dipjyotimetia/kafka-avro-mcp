# kafka-avro-mcp

`kafka-avro-mcp` compiles an Avro event contract plus an explicit Kafka/MCP overlay into safe, fixed-topic MCP producer tools.

```bash
go run ./cmd/avro-gen-go-mcp generate \
  --config examples/orders/kafka.mcp.yaml \
  --out ./gen
```

Generated packages expose one adapter-neutral `RegisterTools` entry point. Wrap an official `modelcontextprotocol/go-sdk` server with `runtime.WrapGoSDK`, or an `mcp-go` server with `runtime.WrapMCPGo`, then supply a `runtime.Service` built from `runtime.RegistryResolver` and `runtime.KafkaPublisher`. `examples/orders/server` is that wiring end to end, over stdio:

```bash
KAFKA_BROKERS=localhost:9092 \
SCHEMA_REGISTRY_URL=http://localhost:8081 \
go run ./examples/orders/server
```

Validate local contracts before generation or use a read-only Schema Registry gate in CI:

```bash
avro-gen-go-mcp validate --config kafka.mcp.yaml
avro-gen-go-mcp validate --config kafka.mcp.yaml --registry-url https://registry.example
avro-gen-go-mcp validate --config kafka.mcp.yaml --registry-url https://registry.example --registry-user ci
```

`--registry-url` and `--registry-user` fall back to `$SCHEMA_REGISTRY_URL` and `$SCHEMA_REGISTRY_USER`. The password is read only from `$SCHEMA_REGISTRY_PASSWORD`, never from a flag, since arguments are visible to anything that can list processes. The example server reads the same three variables.

V1 supports record roots, primitives, nested records, arrays, maps, enums, and nullable unions. It intentionally does not permit caller-controlled topics, schema registration, headers, consumer tools, dynamic discovery, logical types, or non-nullable multi-branch unions.

`RegisterTool` enforces the generated input schema itself rather than relying on the host SDK: the official go-sdk validates only through its generic `AddTool`, and `mcp-go` only when the server opts into `WithInputSchemaValidation`, so arguments outside the advertised schema would otherwise reach the encoder — and an unknown field would be silently dropped rather than rejected. Two consequences are worth knowing: a field counts as optional only when the Avro schema gives it a default (being nullable is not enough, because the encoder still needs a value), and an Avro `bytes` field is advertised and accepted as base64, then decoded before encoding.

Each manifest event declares a Schema Registry subject explicitly. At runtime the resolver performs a lookup only: an unregistered/mismatched schema prevents publication. Publish tools are marked side-effecting, use fixed manifest topics, and default to a 1 MiB ceiling on the produced record (wire header + Avro payload + key); callers can lower or raise it through `runtime.WithMaxMessageBytes`. Kafka additionally charges per-record batch overhead, so the guard is a sanity check rather than an exact predictor of the broker's `max.message.bytes`.

Failures are split by who can act on them. A payload problem — a key field missing, a record over the size ceiling, a value the encoder rejects — goes back to the caller in full so a model can correct itself. A broker or registry failure can carry hostnames and internal addresses, so the tool result names only the topic and the detail goes to the `runtime.WithLogger` logger (`slog.Default()` otherwise).
