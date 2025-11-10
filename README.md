# 🔌 daplug-ddb (da•plug)

> **Schema-Driven DynamoDB Normalization & Event Publishing for Python**

[![CircleCI](https://circleci.com/gh/dual/daplug-ddb.svg?style=shield)](https://circleci.com/gh/dual/daplug-ddb)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=dual_daplug-ddb&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=dual_daplug-ddb)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=dual_daplug-ddb&metric=bugs)](https://sonarcloud.io/summary/new_code?id=dual_daplug-ddb)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=dual_daplug-ddb&metric=coverage)](https://sonarcloud.io/summary/new_code?id=dual_daplug-ddb)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org/downloads/)
[![PyPI package](https://img.shields.io/pypi/v/daplug-ddb?color=blue&label=pypi%20package)](https://pypi.org/project/daplug-ddb/)
[![License](https://img.shields.io/badge/license-apache%202.0-blue)](LICENSE)
[![Contributions](https://img.shields.io/badge/contributions-welcome-blue)](https://github.com/paulcruse3/daplug-ddb/issues)

`daplug-ddb` is a lightweight package that provides schema-aware CRUD helpers, batch utilities, and optional SNS publishing so you can treat DynamoDB as a structured datastore without rewriting boilerplate for every project.

## ✨ Key Features

- **Schema Mapping** – Convert inbound payloads into strongly typed DynamoDB
  items driven by your OpenAPI (or JSON schema) definitions.
- **Idempotent CRUD** – Consistent `create`, `overwrite`, `update`, `delete`,
  and `read` operations with optional optimistic locking via an
  `idempotence_key`.
- **Batch Helpers** – Simplified batch insert/delete flows that validate data
  and handle chunking for you.
- **SNS Integration** – Optional event publishing for every write operation so
  downstream systems stay in sync.

## 🚀 Quick Start

### Installation

```bash
pip install daplug-ddb
# pipenv install daplug-ddb
# poetry add daplug-ddb
# uv pip install daplug-ddb
```

### Basic Usage

```python
import daplug_ddb

adapter = daplug_ddb.adapter(
    table="example-table",
    endpoint="https://dynamodb.us-east-2.amazonaws.com", # optional, will use AWS conventional env vars if using on lambda
    schema_file="openapi.yml",
    hash_key="record_id",
    idempotence_key="modified",
)

item = adapter.create(
    data={
        "record_id": "abc123",
        "object_key": {"string_key": "value"},
        "array_number": [1, 2, 3],
        "modified": "2024-01-01",
    },
    schema="ExampleModel",
)

print(item)
```

Because the adapter is configured with a `schema_file`, every call can opt into
mapping by supplying `schema`. Skip the schema argument when you want to write
the data exactly as provided.

## 🔧 Advanced Configuration

### Selective Updates

```python
# Merge partial updates while preserving existing attributes
adapter.update(
    operation="get",  # fetch original item via get; use "query" for indexes
    query={
        "Key": {"record_id": "abc123", "sort_key": "v1"}
    },
    data={
        "record_id": "abc123",
        "sort_key": "v1",
        "array_number": [1, 2, 3, 4],
    },
    update_list_operation="replace",
)
```

### Hash/Range Prefixing

```python
adapter = daplug_ddb.adapter(
    table="tenant-config",
    endpoint="https://dynamodb.us-east-2.amazonaws.com",
    schema_file="openapi.yml",
    hash_key="tenant_id",
)

prefix_args = {
    "hash_key": "tenant_id",
    "hash_prefix": "tenant#",
    "range_key": "sort_key",
    "range_prefix": "config#",
}

item = adapter.create(
    data={
        "tenant_id": "abc",
        "sort_key": "default",
        "modified": "2024-01-01",
    },
    schema="TenantModel",
    **prefix_args,
)
# DynamoDB stores tenant_id as "tenant#abc", but the adapter returns "abc"
```

When prefixes are provided, the adapter automatically applies them on the way
into DynamoDB (including batch operations and deletes) and removes them before
returning data or publishing SNS events. Pass the same `prefix_args` to reads
(`get`, `query`, `scan`) so query keys are expanded and responses are cleaned.
Codex workflow references:

- [Codex agent guide](.agents/CODEX.md)

### Batched Writes

```python
adapter.batch_insert(
    data=[
        {"record_id": str(idx), "sort_key": str(idx)}
        for idx in range(100)
    ],
    batch_size=25,
)

adapter.batch_delete(
    data=[
        {"record_id": str(idx), "sort_key": str(idx)}
        for idx in range(100)
    ]
)
```

### Idempotent Operations

```python
adapter = daplug_ddb.adapter(
    table="orders",
    endpoint="https://dynamodb.us-east-2.amazonaws.com",
    schema_file="openapi.yml",
    hash_key="order_id",
    idempotence_key="modified",
)

updated = adapter.update(
    data={"order_id": "abc123", "modified": "2024-02-01"},
    operation="get",
    query={"Key": {"order_id": "abc123"}},
    schema="OrderModel",
)
```

The adapter fetches the current item, merges the update, and executes a
conditional `PutItem` to ensure the stored `modified` value still matches
what was read. If another writer changes the record first, the operation
fails with a conditional check error rather than overwriting the data.

Set `raise_idempotence_error=True` if you prefer the adapter to raise a
`ValueError` instead of relying on DynamoDB's conditional failure. Leaving it
at the default (`False`) allows you to detect conflicts without breaking the
update flow.

```python
adapter = daplug_ddb.adapter(
    table="orders",
    schema_file="openapi.yml",
    hash_key="order_id",
    idempotence_key="modified",
    raise_idempotence_error=True,
)
```

Enable `idempotence_use_latest=True` when you want the adapter to keep the
most recent copy based on the timestamp stored in the idempotence key. Stale
updates are ignored automatically.

```python
adapter = daplug_ddb.adapter(
    table="orders",
    schema_file="openapi.yml",
    hash_key="order_id",
    idempotence_key="modified",
    idempotence_use_latest=True,
)
````

Stale updates are short-circuited before DynamoDB writes occur.

```txt
Client Update Request
        │
        ▼
  [Adapter.fetch]
        │  (reads original item)
        ▼
┌──────────────────────────┐
│ Original Item            │
│ modified = "2024-01-01"  │
└──────────────────────────┘
        │ merge + map
        ▼
PutItem rejected → original returned
```

```txt
Client Update Request
        │
        ▼
  [Adapter.fetch]
        │  (reads original item)
        ▼
┌──────────────────────────┐
│ Original Item            │
│ idempotence_key = "v1"   │
└──────────────────────────┘
        │ merge + map
        ▼
PutItem(Item=…, ConditionExpression=Attr(idempotence_key).eq("v1"))
        │
   ┌────┴───────┐
   │            │
   ▼            ▼
Success     ConditionalCheckFailed
          (another writer changed key)
```

### SNS Publishing

### Per-call SNS Attributes

You can supply request-scoped SNS message attributes by passing 'sns_attributes'
into any adapter operation (e.g. 'create', 'update', 'delete'). These merge
with adapter defaults and schema-derived metadata.

```python
adapter = daplug_ddb.adapter(
    table="audit-table",
    schema_file="openapi.yml",
    hash_key="audit_id",
    idempotence_key="version",
    sns_arn="arn:aws:sns:us-east-2:123456789012:audit-events",
    sns_endpoint="https://sns.us-east-2.amazonaws.com",
    sns_attributes={"source": "daplug"},
)
adapter.create(
    data=item,
    schema="AuditModel",
    sns_attributes={"source": "billing", "priority": "high"},
)
# => publishes a formatted SNS event with schema metadata
```

Adapter-level 'sns_attributes' supplied when constructing the adapter act as
defaults for every publish. Use per-call 'sns_attributes' to extend or override
those defaults without touching the adapter configuration. Each publish always
adds an 'operation' attribute reflecting the CRUD action so subscribers can
route by verb.

```python
adapter = daplug_ddb.adapter(
    table="audit-table",
    schema_file="openapi.yml",
    hash_key="audit_id",
    sns_arn="arn:aws:sns:...",
    sns_attributes={"source": "daplug", "env": "prod"},
)

# emits {source: "daplug", env: "prod", operation: "create"}
adapter.create(data=item, schema="AuditModel")

# overrides only the env attribute for this publish
adapter.update(
    data=item,
    schema="AuditModel",
    sns_attributes={"env": "staging"},
)
```

## 📚 Method Reference

Each adapter instance holds shared configuration such as `schema_file`, SNS
defaults, and optional key prefixes. Pass the schema name (and any
operation-specific overrides) when you invoke a method.

```python
adapter = daplug_ddb.adapter(
    table="orders",
    schema_file="openapi.yml",
    hash_key="order_id",
    idempotence_key="modified",
)
```

### `create` (wrapper around `insert`/`overwrite`)

```python
# default: behaves like insert (requires hash_key)
adapter.create(data=payload, schema="OrderModel")

# explicit overwrite (upsert semantics)
adapter.create(
    operation="overwrite",
    data=payload,
    schema="OrderModel",
)
```

### `insert`

```python
adapter.insert(data=payload, schema="OrderModel")
```

### `overwrite`

```python
adapter.overwrite(data=payload, schema="OrderModel")
```

### `get`

```python
adapter.get(
    query={"Key": {"order_id": "abc123"}},
    schema="OrderModel",
)
```

### `query`

```python
adapter.query(
    query={
        "IndexName": "test_query_id",
        "KeyConditionExpression": "test_query_id = :id",
        "ExpressionAttributeValues": {":id": "def345"},
    },
    schema="OrderModel",
)
```

### `scan`

```python
adapter.scan(schema="OrderModel")

# raw DynamoDB response
adapter.scan(raw_scan=True)
```

### `read`

`read` delegates to `get`, `query`, or `scan` based on the
`operation` kwarg.

```python
# single item
adapter.read(operation="get", query={"Key": {"order_id": "abc123"}}, schema="OrderModel")

# query
adapter.read(
    operation="query",
    query={
        "KeyConditionExpression": "test_query_id = :id",
        "ExpressionAttributeValues": {":id": "def345"},
    },
    schema="OrderModel",
)
```

### `update`

```python
adapter.update(
    data={"order_id": "abc123", "modified": "2024-03-02"},
    operation="get",
    query={"Key": {"order_id": "abc123"}},
    schema="OrderModel",
)
```

### `delete`

```python
adapter.delete(query={"Key": {"order_id": "abc123"}})
```

### `batch_insert`

```python
adapter.batch_insert(data=[{...} for _ in range(10)], schema="OrderModel", batch_size=25)
```

### `batch_delete`

```python
adapter.batch_delete(data=[{...} for _ in range(10)], batch_size=25)
```

### Prefixing Helpers

Include per-call prefix overrides whenever you need to scope keys.

```python
adapter.insert(
    data=payload,
    schema="OrderModel",
    hash_key="order_id",
    hash_prefix="tenant#",
)
```

## 🧪 Local Development

### Prerequisites

- Python **3.9+**
- [Pipenv](https://pipenv.pypa.io/)
- Docker (for running DynamoDB Local during tests)

### Environment Setup

```bash
git clone https://github.com/paulcruse3/daplug-ddb.git
cd daplug-ddb
pipenv install --dev
```

### Sync Packaging Metadata

Keep `setup.py` aligned with the locked Pipenv dependencies before publishing.

```bash
pipenv run pipenv-setup sync
```

### Run Tests

```bash
# unit tests (no DynamoDB required)
pipenv run test

# integration tests (spins up local DynamoDB when available)
pipenv run integrations
```

Supplying an `idempotence_key` enables optimistic concurrency for updates and overwrites. The adapter reads the original item, captures the key’s value, and issues a `PutItem` with a `ConditionExpression` asserting the value is unchanged. If another writer updates the record first, DynamoDB returns a conditional check failure instead of silently overwriting data.

```txt
Client Update Request
        │
        ▼
  [Adapter.fetch]
        │  (reads original item)
        ▼
┌──────────────────────────┐
│ Original Item            │
│ idempotence_key = "v1"   │
└──────────────────────────┘
        │ merge + map
        ▼
PutItem(Item=…, ConditionExpression=Attr(idempotence_key).eq("v1"))
        │
   ┌────┴───────┐
   │            │
   ▼            ▼
Success     ConditionalCheckFailed
          (another writer changed key)
```

- **Optional:** Omit `idempotence_key` to mirror DynamoDB’s default “last write wins” behavior while still benefiting from schema normalization.
- **Safety:** When the key is configured but missing on the fetched item, the adapter raises `ValueError`, surfacing misconfigurations early.
- **Events:** SNS notifications include the idempotence metadata so downstream services can reason about version changes.

### Coverage & Linting

```bash
# generates HTML, XML, and JUnit reports under ./coverage/
pipenv run coverage

# pylint configuration aligned with the legacy project
pipenv run lint
```

## 📦 Project Structure

```txt
daplug-ddb/
├── daplug_ddb/
│   ├── adapter.py           # DynamoDB adapter implementation
│   ├── prefixer.py          # DynamoDB prefixer implementation
│   ├── common/              # Shared helpers (merging, schema loading, logging)
│   └── __init__.py          # Public adapter factory & exports
├── tests/
│   ├── integration/         # Integration suite against DynamoDB Local
│   ├── unit/                # Isolated unit tests using mocks
│   └── openapi.yml          # Sample schema used for mapping tests
├── Pipfile                  # Runtime and dev dependencies
├── setup.py                 # Packaging metadata
└── README.md
```

## 🤝 Contributing

Contributions are welcome! Open an issue or submit a pull request if you’d like
to add new features, improve documentation, or expand test coverage.

```bash
git checkout -b feature/amazing-improvement
# make your changes
pipenv run lint
pipenv run test
pipenv run integrations
git commit -am "feat: amazing improvement"
git push origin feature/amazing-improvement
```

## 📄 License

Apache License 2.0 – see [LICENSE](LICENSE) for full text.

---

> Built to keep DynamoDB integrations DRY, predictable, and schema-driven.
