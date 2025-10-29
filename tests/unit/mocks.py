"""Shared mocks and fixtures for unit tests."""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional

from daplug_ddb.types import DynamoItem, DynamoItems


BASE_ITEM: DynamoItem = {
    "test_id": "abc123",
    "test_query_id": "def345",
    "object_key": {"string_key": "nothing"},
    "array_number": [1, 2, 3],
    "array_objects": [{"array_string_key": "a", "array_number_key": 1}],
    "created": "2020-10-05",
    "modified": "2020-10-05",
}


def build_test_item(**overrides: Any) -> DynamoItem:
    item = copy.deepcopy(BASE_ITEM)
    item.update(overrides)
    return item


class StubBatchWriter:
    """Simple context manager capturing batch operations."""

    def __init__(self, table: "StubTable") -> None:
        self.table = table

    def __enter__(self) -> "StubBatchWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[override]
        return False

    def put_item(self, **kwargs: Any) -> None:
        self.table.batch_put_calls.append(kwargs)

    def delete_item(self, **kwargs: Any) -> None:
        self.table.batch_delete_calls.append(kwargs)


class StubTable:
    """In-memory stand-in for a DynamoDB Table object."""

    def __init__(self) -> None:
        self.put_calls: List[Dict[str, Any]] = []
        self.delete_calls: List[Dict[str, Any]] = []
        self.batch_put_calls: List[Dict[str, Any]] = []
        self.batch_delete_calls: List[Dict[str, Any]] = []
        self.get_calls: List[Dict[str, Any]] = []
        self.query_calls: List[Dict[str, Any]] = []
        self.scan_calls: List[Dict[str, Any]] = []
        self.get_item_response: DynamoItem = build_test_item()
        self.query_response: DynamoItems = [build_test_item()]
        self.scan_response: DynamoItems = [build_test_item()]

    # DynamoDB Table API -------------------------------------------------
    def put_item(self, **kwargs: Any) -> Dict[str, Any]:
        self.put_calls.append(kwargs)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def delete_item(self, **kwargs: Any) -> Dict[str, Any]:
        self.delete_calls.append(kwargs)
        return {"Attributes": kwargs.get("Key", {})}

    def get_item(self, **kwargs: Any) -> Dict[str, Any]:
        self.get_calls.append(kwargs)
        return {"Item": copy.deepcopy(self.get_item_response)}

    def scan(self, **kwargs: Any) -> Dict[str, Any]:
        self.scan_calls.append(kwargs)
        return {"Items": copy.deepcopy(self.scan_response)}

    def query(self, **kwargs: Any) -> Dict[str, Any]:
        self.query_calls.append(kwargs)
        return {"Items": copy.deepcopy(self.query_response)}

    def batch_writer(self) -> StubBatchWriter:
        return StubBatchWriter(self)
