# Contributing to kafka-avro-mcp

Thanks for your interest in contributing. Bug reports, fixes, documentation and tests are all welcome.

By participating you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md). To report a security problem, follow [SECURITY.md](SECURITY.md) instead of opening an issue.

## Before you start

- **Bugs:** open an issue with the manifest, Avro schema and command or tool call that reproduces the problem, plus the output you expected.
- **Features:** open an issue to discuss it before writing code. The project deliberately keeps a small, static scope (see [Scope](#scope)), so an early conversation saves wasted work.
- **Small fixes** (typos, docs, obvious bugs) can go straight to a pull request.

## Development setup

You need Go 1.27 or later. Docker is needed only for the integration tests, and [golangci-lint](https://golangci-lint.run) v2 for linting.

```bash
git clone https://github.com/dipjyotimetia/kafka-avro-mcp.git
cd kafka-avro-mcp
go build ./...
go test -race ./...
```

## Making changes

1. Fork the repository and create a branch from `main`.
2. Make your change, with tests. Bug fixes should include a test that fails without the fix.
3. Run the checks below.
4. Open a pull request that explains what changed and why, and links the related issue.

### Checks

CI runs all of these; please run them locally first:

```bash
gofmt -l ./cmd ./pkg ./examples ./integration   # must print nothing
go vet ./...
go test -race ./...
golangci-lint run

# Integration tests: a separate Go module, needs Docker
cd integration && go vet ./... && go test ./... && golangci-lint run
```

### Generated code

`examples/orders/gen/tools.mcp.go` is generated and checked in. CI regenerates it and fails on any difference. If you change the generator, the JSON Schema converter or the example schemas, regenerate it and commit the result. Never edit it by hand.

```bash
go run ./cmd/avro-gen-go-mcp generate --config examples/orders/kafka.mcp.yaml --out examples/orders/gen
```

### Guidelines

- Keep changes focused. Unrelated refactors belong in their own pull request.
- Keep the `pkg/runtime` core free of MCP SDK imports; SDK-specific code belongs in `pkg/runtime/gosdk` or `pkg/runtime/mcpgo`.
- Preserve the error split: `runtime.PayloadError` is returned to the model verbatim, and anything else is logged and reduced to a message that names only the topic.
- Keep payload checks ahead of the Schema Registry lookup.
- Update `README.md` when behaviour, flags or environment variables change.

### Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org), as the existing history does:

```text
fix(runtime): reject unknown fields in nested records
feat(jsonschema): support ...
docs: clarify registry lookup behaviour
```

## Scope

The project intentionally does **not** support caller-controlled topics, schema registration, headers, consumer tools, dynamic discovery, `fixed`, the Avro specification's logical types, or non-nullable multi-branch unions. Proposals to relax these are welcome as issues, but they need a design discussion first, because they change the project's safety guarantees.

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
