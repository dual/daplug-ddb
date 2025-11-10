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
