# 🔌 daplug-ddb

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
    engine="dynamodb",
    table="example-table",
    endpoint="https://dynamodb.us-east-2.amazonaws.com", # optional, will use AWS conventional env vars if using on lambda
    schema="ExampleModel",
    schema_file="openapi.yml",
    identifier="record_id",
    idempotence_key="modified",
)

item = adapter.create(
    data={
        "record_id": "abc123",
        "object_key": {"string_key": "value"},
        "array_number": [1, 2, 3],
        "modified": "2024-01-01",
    }
)

print(item)
```

The adapter automatically maps the payload to your schema and publishes an SNS
event if credentials are provided.

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
    engine="dynamodb",
    table="tenant-config",
    endpoint="https://dynamodb.us-east-2.amazonaws.com",
    schema="TenantModel",
    schema_file="openapi.yml",
    identifier="tenant_id",
    hash_key="tenant_id",
    hash_prefix="tenant#",
    range_key="sort_key",
    range_prefix="config#",
)

item = adapter.create(data={
    "tenant_id": "abc",
    "sort_key": "default",
    "modified": "2024-01-01",
})
# DynamoDB stores tenant_id as "tenant#abc", but the adapter returns "abc"
```

When prefixes are configured, the adapter automatically applies them on the way
into DynamoDB (including batch operations and deletes) and removes them before
returning data or publishing SNS events.

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
    engine="dynamodb",
    table="orders",
    endpoint="https://dynamodb.us-east-2.amazonaws.com",
    schema="OrderModel",
    schema_file="openapi.yml",
    identifier="order_id",
    idempotence_key="modified",
)

updated = adapter.update(
    data={"order_id": "abc123", "modified": "2024-02-01"},
    operation="get",
    query={"Key": {"order_id": "abc123"}},
)
```

The adapter fetches the current item, merges the update, and executes a
conditional `PutItem` to ensure the stored `modified` value still matches
what was read. If another writer changes the record first, the operation
fails with a conditional check error rather than overwriting the data.

```txt
Client Update Request
        │
        ▼
  [Adapter.fetch]
        │  (reads original item)
        ▼
┌──────────────────────────┐
│ Original Item            │
│ idempotence_key = "v1"  │
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

```python
adapter = daplug_ddb.adapter(
    engine="dynamodb",
    table="audit-table",
    schema="AuditModel",
    schema_file="openapi.yml",
    identifier="audit_id",
    idempotence_key="version",
    sns_arn="arn:aws:sns:us-east-2:123456789012:audit-events",
    sns_endpoint="https://sns.us-east-2.amazonaws.com",
    sns_attributes={"source": "daplug"},
)

adapter.delete(
    query={
        "Key": {"audit_id": "abc123", "version": "2024-01-01"}
    }
)
# => publishes a formatted SNS event with schema metadata
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
│ idempotence_key = "v1"  │
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
