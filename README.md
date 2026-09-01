# kafka-avro-mcp

`kafka-avro-mcp` compiles an Avro event contract plus an explicit Kafka/MCP overlay into safe, fixed-topic MCP producer tools.

```bash
go run ./cmd/avro-gen-go-mcp generate \
  --config examples/orders/kafka.mcp.yaml \
  --out ./gen
```

Generated packages expose one adapter-neutral `RegisterTools` entry point. Wrap an official `modelcontextprotocol/go-sdk` server with `runtime.WrapGoSDK`, or an `mcp-go` server with `runtime.WrapMCPGo`, then supply a `runtime.Service` built from `runtime.RegistryResolver` and `runtime.KafkaPublisher`.

Validate local contracts before generation or use a read-only Schema Registry gate in CI:

```bash
avro-gen-go-mcp validate --config kafka.mcp.yaml
avro-gen-go-mcp validate --config kafka.mcp.yaml --registry-url https://registry.example
```

V1 supports record roots, primitives, nested records, arrays, maps, enums, and nullable unions. It intentionally does not permit caller-controlled topics, schema registration, headers, consumer tools, dynamic discovery, logical types, or non-nullable multi-branch unions.

Each manifest event declares a Schema Registry subject explicitly. At runtime the resolver performs a lookup only: an unregistered/mismatched schema prevents publication. Publish tools are marked side-effecting, use fixed manifest topics, and default to a 1 MiB encoded-payload ceiling; callers can lower or raise it through `runtime.WithMaxMessageBytes`.
