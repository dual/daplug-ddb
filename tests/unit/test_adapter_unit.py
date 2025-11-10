"""Unit tests for the DynamoDB adapter with mocked boto3 interactions."""

from __future__ import annotations

import importlib
from unittest.mock import patch

import pytest

import daplug_ddb
from daplug_ddb.adapter import DynamodbAdapter
from daplug_ddb.exception import BatchItemException
from daplug_core.base_adapter import BaseAdapter

from tests.unit.mocks import StubTable, build_test_item

SCHEMA_ARGS = {"schema": "test-dynamo-model"}
PREFIX_ARGS = {
    "hash_key": "test_id",
    "hash_prefix": "tenant#",
    "range_key": "test_query_id",
    "range_prefix": "type#",
}


def _create_adapter(table: StubTable, **overrides) -> DynamodbAdapter:
    params = {
        "table": "stub-table",
        "endpoint": None,
        "schema_file": "tests/openapi.yml",
        "hash_key": "test_id",
    }
    params.update(overrides)
    adapter_module = importlib.import_module("daplug_ddb.adapter")
    with patch.object(adapter_module.boto3, "resource") as resource:
        resource.return_value.Table.return_value = table
        return daplug_ddb.adapter(**params)


def test_insert_applies_hash_key_condition() -> None:
    table = StubTable()
    adapter = _create_adapter(table)
    adapter.insert(data=build_test_item(), **SCHEMA_ARGS)

    assert table.put_calls, "put_item should be invoked"
    call_kwargs = table.put_calls[-1]
    assert "ConditionExpression" in call_kwargs
    assert call_kwargs["Item"]["test_id"] == "abc123"


def test_update_without_idempotence_key_omits_condition_expression() -> None:
    table = StubTable()
    table.get_item_response = build_test_item()
    adapter = _create_adapter(table)

    updated = build_test_item(array_number=[1, 2, 3, 4])
    adapter.update(
        data=updated,
        operation="get",
        query={"Key": {"test_id": "abc123", "test_query_id": "def345"}},
        **SCHEMA_ARGS,
    )

    call_kwargs = table.put_calls[-1]
    assert "ConditionExpression" not in call_kwargs
    assert call_kwargs["Item"]["array_number"] == [1, 2, 3, 4]


def test_update_with_idempotence_key_sets_condition_expression() -> None:
    table = StubTable()
    table.get_item_response = build_test_item(modified="2020-10-05")
    adapter = _create_adapter(table, idempotence_key="modified")

    updated = build_test_item(modified="2020-10-06")
    adapter.update(
        data=updated,
        operation="get",
        query={"Key": {"test_id": "abc123", "test_query_id": "def345"}},
        **SCHEMA_ARGS,
    )

    call_kwargs = table.put_calls[-1]
    assert call_kwargs.get("ConditionExpression") is not None


def test_update_with_missing_idempotence_value_skips_condition() -> None:
    table = StubTable()
    table.get_item_response = build_test_item()
    adapter = _create_adapter(table, idempotence_key="missing_key")

    adapter.update(
        data=build_test_item(),
        operation="get",
        query={"Key": {"test_id": "abc123", "test_query_id": "def345"}},
        **SCHEMA_ARGS,
    )

    call_kwargs = table.put_calls[-1]
    assert "ConditionExpression" not in call_kwargs


def test_batch_insert_rejects_non_list_input() -> None:
    table = StubTable()
    adapter = _create_adapter(table)

    with pytest.raises(BatchItemException):
        adapter.batch_insert(data=(1, 2, 3), **SCHEMA_ARGS)


def test_update_raises_when_idempotence_value_changes_and_flag_set() -> None:
    table = StubTable()
    table.get_item_response = build_test_item(modified="2020-01-01")
    adapter = _create_adapter(table, idempotence_key="modified", raise_idempotence_error=True)

    with pytest.raises(ValueError):
        adapter.update(
            data=build_test_item(modified="2020-02-01"),
            operation="get",
            query={"Key": {"test_id": "abc123", "test_query_id": "def345"}},
            **SCHEMA_ARGS,
        )


def test_update_allows_mismatched_idempotence_when_flag_false() -> None:
    table = StubTable()
    table.get_item_response = build_test_item(modified="2020-01-01")
    adapter = _create_adapter(table, idempotence_key="modified", raise_idempotence_error=False)

    adapter.update(
        data=build_test_item(modified="2020-02-01"),
        operation="get",
        query={"Key": {"test_id": "abc123", "test_query_id": "def345"}},
        **SCHEMA_ARGS,
    )

    call_kwargs = table.put_calls[-1]
    assert call_kwargs["Item"]["modified"] == "2020-02-01"


def test_update_use_latest_ignores_stale_payload() -> None:
    table = StubTable()
    table.get_item_response = build_test_item(modified="2020-02-01")
    adapter = _create_adapter(
        table,
        idempotence_key="modified",
        idempotence_use_latest=True,
    )

    result = adapter.update(
        data=build_test_item(modified="2020-01-01"),
        operation="get",
        query={"Key": {"test_id": "abc123", "test_query_id": "def345"}},
        **SCHEMA_ARGS,
    )

    assert result["modified"] == "2020-02-01"
    assert table.put_calls == []


def test_update_use_latest_raises_on_invalid_date() -> None:
    table = StubTable()
    table.get_item_response = build_test_item(modified="2020-02-01")
    adapter = _create_adapter(
        table,
        idempotence_key="modified",
        idempotence_use_latest=True,
    )

    with pytest.raises(ValueError):
        adapter.update(
            data=build_test_item(modified="not-a-date"),
            operation="get",
            query={"Key": {"test_id": "abc123", "test_query_id": "def345"}},
            **SCHEMA_ARGS,
        )


def test_base_adapter_merges_sns_attributes() -> None:
    base = BaseAdapter(
        sns_attributes={"custom": "value", "override": "adapter"},
    )

    formatted = base.create_format_attributes({"call": "value", "override": "call"})

    assert formatted["custom"]["StringValue"] == "value"
    assert formatted["call"]["StringValue"] == "value"
    assert formatted["override"]["StringValue"] == "call"


def test_publish_uses_provided_sns_attributes() -> None:
    table = StubTable()
    adapter = _create_adapter(table, sns_arn="arn:aws:sns:::example")

    with patch.object(adapter.publisher, "publish") as publish:
        adapter.insert(
            data=build_test_item(),
            sns_attributes={"schema": SCHEMA_ARGS["schema"]},
            **SCHEMA_ARGS,
        )

    assert publish.call_count == 1
    attributes = publish.call_args.kwargs["attributes"]
    assert attributes["schema"]["StringValue"] == SCHEMA_ARGS["schema"]


def test_publish_merges_adapter_and_call_attributes() -> None:
    table = StubTable()
    adapter = _create_adapter(
        table,
        sns_arn="arn:aws:sns:::example",
        sns_attributes={"source": "adapter", "override": "adapter"},
    )

    with patch.object(adapter.publisher, "publish") as publish:
        adapter.insert(
            data=build_test_item(),
            sns_attributes={"override": "call"},
            **SCHEMA_ARGS,
        )

    attributes = publish.call_args.kwargs["attributes"]
    assert attributes["source"]["StringValue"] == "adapter"
    assert attributes["override"]["StringValue"] == "call"


def test_insert_requires_hash_key() -> None:
    table = StubTable()
    adapter = _create_adapter(table, hash_key=None)

    with pytest.raises(ValueError):
        adapter.insert(data=build_test_item(), **SCHEMA_ARGS)


def test_create_defaults_to_insert() -> None:
    table = StubTable()
    adapter = _create_adapter(table)

    adapter.create(data=build_test_item(), **SCHEMA_ARGS)

    call_kwargs = table.put_calls[-1]
    assert "ConditionExpression" in call_kwargs


def test_create_without_hash_key_overwrites() -> None:
    table = StubTable()
    adapter = _create_adapter(table, hash_key=None)

    with pytest.raises(ValueError):
        adapter.create(data=build_test_item(), **SCHEMA_ARGS)


def test_insert_applies_configured_prefixes() -> None:
    table = StubTable()
    adapter = _create_adapter(table)

    result = adapter.insert(data=build_test_item(), **SCHEMA_ARGS, **PREFIX_ARGS)

    stored_item = table.put_calls[-1]["Item"]
    assert stored_item["test_id"] == "tenant#abc123"
    assert stored_item["test_query_id"] == "type#def345"
    assert result["test_id"] == "abc123"


def test_get_removes_configured_prefixes() -> None:
    table = StubTable()
    table.get_item_response = {
        "test_id": "tenant#abc123",
        "test_query_id": "type#def345",
        "modified": "2020-10-05",
    }
    adapter = _create_adapter(table)

    item = adapter.get(
        query={"Key": {"test_id": "abc123", "test_query_id": "def345"}},
        **PREFIX_ARGS,
    )

    stored_key = table.get_calls[-1]["Key"]
    assert stored_key["test_id"] == "tenant#abc123"
    assert stored_key["test_query_id"] == "type#def345"
    assert item["test_id"] == "abc123"
    assert item["test_query_id"] == "def345"


def test_update_applies_prefixes() -> None:
    table = StubTable()
    table.get_item_response = build_test_item()
    adapter = _create_adapter(table)

    updated = build_test_item(modified="2020-10-06")
    adapter.update(
        data=updated,
        operation="get",
        query={"Key": {"test_id": "abc123", "test_query_id": "def345"}},
        **SCHEMA_ARGS,
        **PREFIX_ARGS,
    )

    stored_item = table.put_calls[-1]["Item"]
    assert stored_item["test_id"] == "tenant#abc123"
    assert stored_item["test_query_id"] == "type#def345"


def test_query_prefixes_expression_attribute_values() -> None:
    table = StubTable()
    table.query_response = [
        {
            "test_id": "tenant#abc123",
            "test_query_id": "type#def345",
        }
    ]
    adapter = _create_adapter(table)

    result = adapter.query(
        query={
            "IndexName": "test_query_id",
            "KeyConditionExpression": "test_id = :test_id",
            "ExpressionAttributeValues": {":test_id": "abc123"},
        },
        **SCHEMA_ARGS,
        **PREFIX_ARGS,
    )

    call = table.query_calls[-1]
    assert call["ExpressionAttributeValues"][":test_id"] == "tenant#abc123"
    assert isinstance(result, list)
    assert result[0]["test_id"] == "abc123"


def test_query_expression_aliases_receive_prefix() -> None:
    table = StubTable()
    table.query_response = [
        {
            "test_id": "tenant#abc123",
            "test_query_id": "type#def345",
        }
    ]
    adapter = _create_adapter(table)

    adapter.query(
        query={
            "IndexName": "test_query_id",
            "KeyConditionExpression": "#pk = :pk",
            "ExpressionAttributeNames": {"#pk": "test_id"},
            "ExpressionAttributeValues": {":pk": "abc123"},
        },
        **SCHEMA_ARGS,
        **PREFIX_ARGS,
    )

    call = table.query_calls[-1]
    assert call["ExpressionAttributeValues"][":pk"] == "tenant#abc123"


def test_create_operation_overwrite_dispatches() -> None:
    table = StubTable()
    adapter = _create_adapter(table, schema_file=None)
    payload = build_test_item()

    with patch.object(adapter, "overwrite", wraps=adapter.overwrite) as overwrite:
        adapter.create(operation="overwrite", data=payload)

    assert overwrite.call_count == 1


def test_read_dispatches_scan() -> None:
    table = StubTable()
    adapter = _create_adapter(table)

    with patch.object(adapter, "scan", wraps=adapter.scan) as scan:
        result = adapter.read(operation="scan", raw_scan=True, **SCHEMA_ARGS)

    assert scan.call_count == 1
    assert "Items" in result


def test_read_dispatches_query() -> None:
    table = StubTable()
    adapter = _create_adapter(table)

    with patch.object(adapter, "query", wraps=adapter.query) as query_call:
        result = adapter.read(operation="query", raw_query=True, **SCHEMA_ARGS)

    assert query_call.call_count == 1
    assert "Items" in result


def test_scan_raw_response_without_prefixer() -> None:
    table = StubTable()
    adapter = _create_adapter(table)

    result = adapter.scan(raw_scan=True, **SCHEMA_ARGS)

    assert result["Items"]


def test_scan_applies_prefixes_and_query_arguments() -> None:
    table = StubTable()
    table.scan_response = [
        {
            "test_id": "tenant#abc123",
            "test_query_id": "type#def345",
        }
    ]
    adapter = _create_adapter(table)
    query = {
        "FilterExpression": "#pk = :pk",
        "ExpressionAttributeNames": {"#pk": "test_id"},
        "ExpressionAttributeValues": {":pk": "abc123"},
    }

    items = adapter.scan(query=query, **SCHEMA_ARGS, **PREFIX_ARGS)

    call = table.scan_calls[-1]
    assert call["ExpressionAttributeValues"][":pk"] == "tenant#abc123"
    assert items[0]["test_id"] == "abc123"


def test_query_raw_response() -> None:
    table = StubTable()
    adapter = _create_adapter(table)

    result = adapter.query(raw_query=True, query={"IndexName": "test_query_id"}, **SCHEMA_ARGS)

    assert "Items" in result


def test_overwrite_returns_unprefixed_payload() -> None:
    table = StubTable()
    adapter = _create_adapter(table, schema_file=None)
    payload = build_test_item()
    with patch.object(adapter.publisher, "publish") as publish:
        result = adapter.overwrite(data=payload)
    assert table.put_calls[-1]["Item"] == payload
    assert result == payload
    assert publish.call_count == 1


def test_batch_insert_applies_prefixes_and_batches() -> None:
    table = StubTable()
    adapter = _create_adapter(table)
    items = [
        {"test_id": str(idx), "test_query_id": str(idx)}
        for idx in range(5)
    ]

    adapter.batch_insert(data=items, batch_size=2, **SCHEMA_ARGS, **PREFIX_ARGS)

    stored = [call["Item"] for call in table.batch_put_calls]
    assert len(stored) == len(items)
    assert stored[0]["test_id"].startswith("tenant#")


def test_delete_with_prefixes_returns_clean_item() -> None:
    table = StubTable()
    adapter = _create_adapter(table)
    with patch.object(adapter.publisher, "publish") as publish:
        deleted = adapter.delete(
            query={"Key": {"test_id": "abc123", "test_query_id": "def345"}},
            **SCHEMA_ARGS,
            **PREFIX_ARGS,
        )
    assert deleted["test_id"] == "abc123"
    assert table.delete_calls[-1]["Key"]["test_id"] == "tenant#abc123"
    assert publish.call_count == 1


def test_batch_delete_applies_prefixes() -> None:
    table = StubTable()
    adapter = _create_adapter(table)
    items = [
        {"test_id": str(idx), "test_query_id": f"id-{idx}"}
        for idx in range(3)
    ]

    adapter.batch_delete(data=items, **SCHEMA_ARGS, **PREFIX_ARGS)

    keys = [call["Key"] for call in table.batch_delete_calls]
    assert len(keys) == len(items)
    assert keys[0]["test_id"].startswith("tenant#")


def test_build_put_kwargs_missing_original_raises() -> None:
    table = StubTable()
    adapter = _create_adapter(
        table,
        idempotence_key="modified",
        raise_idempotence_error=True,
    )

    with pytest.raises(ValueError):
        adapter._DynamodbAdapter__build_put_kwargs(None, {"modified": "2024-01-01"})  # type: ignore[attr-defined]


def test_get_original_data_query_returns_first_item() -> None:
    table = StubTable()
    table.query_response = [
        build_test_item(test_id="first"),
        build_test_item(test_id="second"),
    ]
    adapter = _create_adapter(table)

    result = adapter._DynamodbAdapter__get_original_data(  # type: ignore[attr-defined]
        operation="query",
        query={"IndexName": "test_query_id"},
        **SCHEMA_ARGS,
    )

    assert result["test_id"] == "first"


def test_get_original_data_query_missing_raises() -> None:
    table = StubTable()
    table.query_response = []
    adapter = _create_adapter(table)

    with pytest.raises(ValueError):
        adapter._DynamodbAdapter__get_original_data(  # type: ignore[attr-defined]
            operation="query",
            query={"IndexName": "test_query_id"},
            **SCHEMA_ARGS,
        )


def test_get_original_data_get_missing_raises() -> None:
    table = StubTable()
    table.get_item_response = {}
    adapter = _create_adapter(table)

    with pytest.raises(ValueError):
        adapter._DynamodbAdapter__get_original_data(  # type: ignore[attr-defined]
            operation="get",
            query={"Key": {"test_id": "abc123"}},
            **SCHEMA_ARGS,
        )


def test_should_use_latest_returns_false_when_values_missing() -> None:
    table = StubTable()
    adapter = _create_adapter(
        table,
        idempotence_key="modified",
        idempotence_use_latest=True,
    )

    assert adapter._DynamodbAdapter__should_use_latest(None, "2024-01-01") is False  # type: ignore[attr-defined]


def test_insert_without_schema_uses_raw_payload() -> None:
    table = StubTable()
    adapter = _create_adapter(table, schema_file=None)
    payload = build_test_item()
    with patch.object(adapter.publisher, "publish") as publish:
        adapter.insert(data=payload)
    stored = table.put_calls[-1]["Item"]
    assert stored == payload
    assert publish.call_count == 1


def test_prepare_request_arguments_returns_copy_without_prefix() -> None:
    table = StubTable()
    adapter = _create_adapter(table)
    prefixer = adapter._DynamodbAdapter__build_prefixer({})  # type: ignore[attr-defined]
    query = {"Key": {"test_id": "abc123"}}

    result = adapter._DynamodbAdapter__prepare_request_arguments(  # type: ignore[attr-defined]
        prefixer,
        {"query": query},
    )

    assert result == query
    assert result is not query


def test_prepare_request_arguments_invalid_query_returns_empty() -> None:
    table = StubTable()
    adapter = _create_adapter(table)
    prefixer = adapter._DynamodbAdapter__build_prefixer({})  # type: ignore[attr-defined]

    result = adapter._DynamodbAdapter__prepare_request_arguments(  # type: ignore[attr-defined]
        prefixer,
        {"query": "invalid"},
    )

    assert result == {}


def test_prefixing_enabled_property_reflects_configuration() -> None:
    table = StubTable()
    adapter = _create_adapter(table, hash_prefix="tenant#")

    assert adapter.prefixing_enabled is True


def test_read_defaults_to_get_operation() -> None:
    table = StubTable()
    adapter = _create_adapter(table)
    with patch.object(adapter, "get", wraps=adapter.get) as get_call:
        adapter.read(query={"Key": {"test_id": "abc123"}}, **SCHEMA_ARGS)
    assert get_call.call_count == 1


def test_scan_raw_response_with_prefixer_enabled() -> None:
    table = StubTable()
    table.scan_response = [
        {"test_id": "tenant#abc123", "test_query_id": "type#def345"}
    ]
    adapter = _create_adapter(table)

    result = adapter.scan(raw_scan=True, **SCHEMA_ARGS, **PREFIX_ARGS)

    assert "Items" in result
    assert result["Items"][0]["test_id"] == "abc123"


def test_query_raw_response_with_prefixer_enabled() -> None:
    table = StubTable()
    table.query_response = [
        {"test_id": "tenant#abc123", "test_query_id": "type#def345"}
    ]
    adapter = _create_adapter(table)

    result = adapter.query(raw_query=True, query={"IndexName": "test_query_id"}, **SCHEMA_ARGS, **PREFIX_ARGS)

    assert "Items" in result
    assert result["Items"][0]["test_id"] == "abc123"


def test_batch_insert_without_prefixes_stores_raw_items() -> None:
    table = StubTable()
    adapter = _create_adapter(table, hash_prefix=None, range_prefix=None)
    payload = [{"test_id": "1", "test_query_id": "a"}]

    adapter.batch_insert(data=payload, batch_size=1, **SCHEMA_ARGS)

    assert table.batch_put_calls[-1]["Item"]["test_id"] == "1"


def test_delete_without_prefix_returns_raw_attributes() -> None:
    table = StubTable()
    adapter = _create_adapter(table, hash_prefix=None, range_prefix=None)
    with patch.object(adapter.publisher, "publish") as publish:
        deleted = adapter.delete(query={"Key": {"test_id": "abc123"}}, **SCHEMA_ARGS)
    assert deleted["test_id"] == "abc123"
    assert publish.call_count == 1


def test_batch_delete_rejects_non_list_data() -> None:
    table = StubTable()
    adapter = _create_adapter(table)

    with pytest.raises(BatchItemException):
        adapter.batch_delete(data=("invalid",), **SCHEMA_ARGS)


def test_batch_delete_without_prefix_uses_raw_keys() -> None:
    table = StubTable()
    adapter = _create_adapter(table, hash_prefix=None, range_prefix=None)
    items = [{"test_id": "x", "test_query_id": "y"}]

    adapter.batch_delete(data=items, **SCHEMA_ARGS)

    assert table.batch_delete_calls[-1]["Key"]["test_id"] == "x"


def test_get_original_data_query_handles_raw_response() -> None:
    table = StubTable()
    adapter = _create_adapter(table)
    result = adapter._DynamodbAdapter__get_original_data(  # type: ignore[attr-defined]
        operation="query",
        raw_query=True,
        query={"IndexName": "test_query_id"},
        **SCHEMA_ARGS,
    )

    assert result["test_id"] == "abc123"


def test_get_original_data_query_handles_invalid_result_type() -> None:
    table = StubTable()
    adapter = _create_adapter(table)
    with patch.object(adapter, "query", return_value="not-a-sequence"):
        with pytest.raises(ValueError):
            adapter._DynamodbAdapter__get_original_data(  # type: ignore[attr-defined]
                operation="query",
                query={"IndexName": "test_query_id"},
                **SCHEMA_ARGS,
            )
