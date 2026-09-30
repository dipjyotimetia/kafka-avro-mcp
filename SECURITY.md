# Security Policy

`kafka-avro-mcp` sits between an AI model and a Kafka cluster, so we take security reports seriously.

## Supported versions

The project is pre-1.0. Security fixes land on `main` and in the next release; older versions are not patched.

## Reporting a vulnerability

**Please do not open a public issue, discussion or pull request for a security problem.**

Report it privately through GitHub's [private vulnerability reporting](https://github.com/dipjyotimetia/kafka-avro-mcp/security/advisories/new). Include:

- a description of the issue and its impact,
- the affected version or commit,
- steps or a minimal manifest/schema to reproduce it,
- any suggested fix, if you have one.

You can expect an acknowledgement within 5 working days. We will keep you informed as we investigate, agree a disclosure date with you, and credit you in the advisory unless you prefer otherwise.

## Scope

Reports are especially welcome for anything that breaks the project's safety guarantees, for example:

- a tool call publishing to a topic other than the one fixed in the manifest,
- arguments outside the generated input schema reaching the Avro encoder,
- a schema being registered in the Schema Registry, rather than only looked up,
- broker or registry details (hostnames, addresses, credentials) leaking into a tool result,
- payload contents written to the audit log,
- credentials being accepted from, or exposed through, command-line arguments.

Vulnerabilities in dependencies (franz-go, the MCP SDKs, the Avro library) should be reported upstream; tell us too if `kafka-avro-mcp` needs to change in response.
