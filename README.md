# kafka-avro-mcp

`kafka-avro-mcp` compiles an Avro event contract plus an explicit Kafka/MCP overlay into safe, fixed-topic MCP producer tools.

```bash
go run ./cmd/avro-gen-go-mcp generate \
  --config examples/orders/kafka.mcp.yaml \
  --out ./gen
```

Generated packages expose `RegisterGoSDK` for `modelcontextprotocol/go-sdk` and `RegisterMCPGo` for `mark3labs/mcp-go`. Supply a `runtime.Service` built from `runtime.RegistryResolver` and `runtime.KafkaPublisher`.

V1 supports record roots, primitives, nested records, arrays, maps, enums, and nullable unions. It intentionally does not permit caller-controlled topics, schema registration, headers, consumer tools, dynamic discovery, logical types, or non-nullable multi-branch unions.

Each manifest event declares a Schema Registry subject explicitly. At runtime the resolver performs a lookup only: an unregistered/mismatched schema prevents publication.
