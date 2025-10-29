"""Unit tests for the DynamoDB adapter with mocked boto3 interactions."""

from __future__ import annotations

import importlib
from unittest.mock import patch

import pytest

import daplug_ddb
from daplug_ddb.adapter import DynamodbAdapter
from daplug_ddb.exception import BatchItemException

from tests.unit.mocks import StubTable, build_test_item


def _create_adapter(table: StubTable, **overrides) -> DynamodbAdapter:
    params = {
        "engine": "dynamodb",
        "table": "stub-table",
        "endpoint": None,
        "schema": "test-dynamo-model",
        "schema_file": "tests/openapi.yml",
        "identifier": "test_id",
    }
    params.update(overrides)
    adapter_module = importlib.import_module("daplug_ddb.adapter")
    with patch.object(adapter_module.boto3, "resource") as resource:
        resource.return_value.Table.return_value = table
        return daplug_ddb.adapter(**params)


def test_insert_applies_identifier_condition() -> None:
    table = StubTable()
    adapter = _create_adapter(table)
    adapter.insert(data=build_test_item())

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
    )

    call_kwargs = table.put_calls[-1]
    assert call_kwargs.get("ConditionExpression") is not None


def test_update_with_missing_idempotence_value_raises() -> None:
    table = StubTable()
    table.get_item_response = build_test_item()
    adapter = _create_adapter(table, idempotence_key="missing_key")

    with pytest.raises(ValueError):
        adapter.update(
            data=build_test_item(),
            operation="get",
            query={"Key": {"test_id": "abc123", "test_query_id": "def345"}},
        )


def test_batch_insert_rejects_non_list_input() -> None:
    table = StubTable()
    adapter = _create_adapter(table)

    with pytest.raises(BatchItemException):
        adapter.batch_insert(data=(1, 2, 3))


def test_insert_applies_configured_prefixes() -> None:
    table = StubTable()
    adapter = _create_adapter(
        table,
        hash_key="test_id",
        hash_prefix="tenant#",
        range_key="test_query_id",
        range_prefix="type#",
    )

    result = adapter.insert(data=build_test_item())

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
    adapter = _create_adapter(
        table,
        hash_key="test_id",
        hash_prefix="tenant#",
        range_key="test_query_id",
        range_prefix="type#",
    )

    item = adapter.get(query={"Key": {"test_id": "abc123", "test_query_id": "def345"}})

    stored_key = table.get_calls[-1]["Key"]
    assert stored_key["test_id"] == "tenant#abc123"
    assert stored_key["test_query_id"] == "type#def345"
    assert item["test_id"] == "abc123"
    assert item["test_query_id"] == "def345"


def test_update_applies_prefixes() -> None:
    table = StubTable()
    table.get_item_response = build_test_item()
    adapter = _create_adapter(
        table,
        hash_key="test_id",
        hash_prefix="tenant#",
        range_key="test_query_id",
        range_prefix="type#",
    )

    updated = build_test_item(modified="2020-10-06")
    adapter.update(
        data=updated,
        operation="get",
        query={"Key": {"test_id": "abc123", "test_query_id": "def345"}},
    )

    stored_item = table.put_calls[-1]["Item"]
    assert stored_item["test_id"] == "tenant#abc123"
    assert stored_item["test_query_id"] == "type#def345"
