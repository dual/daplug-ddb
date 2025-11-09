# Agent Usage Tutorial

This guide targets automation agents that need to **use** `daplug-ddb` as a client
library. Mirror these patterns verbatim when generating sample code or wiring the
adapter inside another service.

## 1. Install Runtime Dependencies

```bash
pip install daplug-ddb boto3
# or, inside this repo: pipenv run pip install -e .
```

Agents should assume boto3 credentials come from the runtime environment (AWS
profile, IAM role, etc.). Only pass the `endpoint` kwarg when targeting
LocalStack/DynamoDB Local.

## 2. Bootstrap the Adapter

```python
import daplug_ddb

adapter = daplug_ddb.adapter(
    table="orders",
    schema_file="openapi.yml",   # optional but unlocks schema mapping
    hash_key="order_id",
    idempotence_key="modified",  # enables optimistic concurrency
    sns_arn="arn:aws:sns:us-east-1:123456789012:orders",
    sns_attributes={"source": "checkout"},
)
```

Required knobs:
- `table`: DynamoDB table name.
- `hash_key`: partition key attribute for conditional inserts.

Optional but common:
- `schema_file`: OpenAPI/JSON schema file used for mapping.
- `idempotence_key`: attribute used to gate updates/overwrites.
- `sns_*`: ARN, endpoint, and attribute defaults for publish hooks.

## 3. Create & Read Items (Schema-Aware)

```python
payload = {
    "order_id": "abc123",
    "status": "pending",
    "line_items": [{"sku": "sku-1", "qty": 2}],
    "modified": "2024-05-01T12:00:00",
}

created = adapter.create(data=payload, schema="Order")

fetched = adapter.get(
    query={"Key": {"order_id": "abc123"}},
    schema="Order",
)
```

When `schema` is provided, the adapter maps the payload to the schema-defined
shape (dropping extraneous keys, coercing nested structures). Omit `schema` to
write the payload verbatim.

## 4. Updates with Idempotence

```python
updated = adapter.update(
    data={"order_id": "abc123", "status": "shipped", "modified": "2024-05-02"},
    operation="get",  # fetch original via GetItem
    query={"Key": {"order_id": "abc123"}},
    schema="Order",
)
```

Behavior:
- Adapter fetches the original item, merges fields, and maps via schema.
- `idempotence_key` ensures the stored `modified` value matches the fetched
  copy; mismatches cause DynamoDB conditional failures (or ValueError when
  `raise_idempotence_error=True`).
- Set `idempotence_use_latest=True` to auto-keep whichever timestamp is newer.

## 5. Key Prefixing (Multi-Tenant Patterns)

```python
tenant_adapter = daplug_ddb.adapter(
    table="tenant-config",
    hash_key="tenant_id",
    hash_prefix="tenant#",
    range_key="sort_key",
    range_prefix="config#",
)

item = tenant_adapter.create(
    data={"tenant_id": "abc", "sort_key": "default"},
    schema="TenantConfig",
)
# DynamoDB stores tenant_id as "tenant#abc"; returned value strips prefix.

tenant_adapter.get(
    query={"Key": {"tenant_id": "abc", "sort_key": "default"}},
)
```

Per-call overrides (`hash_prefix`, `range_prefix`) let agents mix prefixed and
plain operations without rebuilding the adapter.

## 6. Batch Insert/Delete

```python
items = [
    {"order_id": f"batch-{idx}", "status": "queued"}
    for idx in range(50)
]

adapter.batch_insert(data=items, schema="Order", batch_size=25)

adapter.batch_delete(
    data=[{"order_id": item["order_id"]} for item in items],
    batch_size=25,
)
```

Rules:
- `data` must be a list; otherwise `BatchItemException` is raised.
- Batch helpers respect schema mapping and prefixing automatically.

## 7. SNS Event Publishing

Every write/delete operation calls `BaseAdapter.publish`. Use it to emit
structured notifications:

```python
adapter = daplug_ddb.adapter(
    table="audit",
    hash_key="audit_id",
    sns_arn="arn:aws:sns:...:audit-events",
    sns_attributes={"source": "daplug"},
)

adapter.create(
    data=audit_payload,
    schema="Audit",
    sns_attributes={"priority": "high"},
)
```

Attribute merge order: adapter defaults → per-call overrides → automatic
`operation`. Consumers downstream can route by any attribute.

## 8. Recommended Patterns for Agents

- **Schema-first**: always pass `schema` when a schema file exists to guarantee
  shaped writes and reads.
- **Queries**: use `adapter.read(operation="query", query={...}, schema=...)`
  when you need direct `Query/Scan` responses but still want schema-cleaned
  items.
- **Raw DynamoDB response**: set `raw_query=True` or `raw_scan=True` to receive
  untouched DynamoDB payloads (pagination tokens, consumed capacity, etc.).
- **Conflict handling**: expose `idempotence_use_latest=True` for APIs where the
  latest timestamp should always win; otherwise propagate conditional check
  failures to callers.

Use this tutorial as the canonical reference when crafting agent responses or
automation scripts that rely on `daplug-ddb` as a client library.
