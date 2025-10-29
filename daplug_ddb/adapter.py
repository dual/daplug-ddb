"""DynamoDB adapter exposing normalized CRUD operations."""

from functools import lru_cache
from typing import Any, Dict, Iterable, Optional, Union, cast

import boto3
from boto3.dynamodb.conditions import Attr

from daplug_ddb.types import DynamoItem, DynamoItems

from .common import map_to_schema, merge
from .common.base_adapter import BaseAdapter
from .exception import BatchItemException


class DynamodbAdapter(BaseAdapter):
    """Implements DynamoDB CRUD operations with schema normalization."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.table = self._get_dynamo_table(kwargs["table"], kwargs.get("endpoint"))
        self.schema_file: str = kwargs["schema_file"]
        self.schema: str = kwargs["schema"]
        self.identifier: str = kwargs["identifier"]
        self.idempotence_key: Optional[str] = kwargs.get("idempotence_key")

    @lru_cache(maxsize=128)
    def _get_dynamo_table(self, table: str, endpoint: Optional[str] = None) -> Any:
        return boto3.resource("dynamodb", endpoint_url=endpoint).Table(table)

    def create(self, **kwargs: Any) -> DynamoItem:
        if kwargs.get("operation") == "overwrite":
            return self.overwrite(**kwargs)
        return self.insert(**kwargs)

    def read(self, **kwargs: Any) -> Union[DynamoItem, DynamoItems, Dict[str, Any]]:
        if kwargs.get("operation") == "query":
            return self.query(**kwargs)
        if kwargs.get("operation") == "scan":
            return self.scan(**kwargs)
        return self.get(**kwargs)

    def scan(self, **kwargs: Any) -> Union[DynamoItems, Dict[str, Any]]:
        if kwargs.get("raw_scan"):
            return self.table.scan(**kwargs.get("query", {}))
        return self.table.scan(**kwargs.get("query", {})).get("Items", [])

    def get(self, **kwargs: Any) -> DynamoItem:
        result: Dict[str, Any] = self.table.get_item(**kwargs.get("query", {}))
        return result.get("Item", {})

    def query(self, **kwargs: Any) -> Union[DynamoItems, Dict[str, Any]]:
        if kwargs.get("raw_query"):
            return self.table.query(**kwargs.get("query", {}))
        return self.table.query(**kwargs.get("query", {})).get("Items", [])

    def overwrite(self, **kwargs: Any) -> DynamoItem:
        overwrite_item = map_to_schema(kwargs["data"], self.schema_file, self.schema)
        self.table.put_item(Item=overwrite_item)
        super().publish("create", overwrite_item, **kwargs)
        return overwrite_item

    def insert(self, **kwargs: Any) -> DynamoItem:
        new_item = map_to_schema(kwargs["data"], self.schema_file, self.schema)
        self.table.put_item(
            Item=new_item, ConditionExpression=Attr(self.identifier).not_exists()
        )
        super().publish("create", new_item, **kwargs)
        return new_item

    def batch_insert(self, **kwargs: Any) -> None:
        data = kwargs["data"]
        batch_size: int = kwargs.get("batch_size", 25)

        if not isinstance(data, list):
            raise BatchItemException("Batched data must be contained within a list")

        batched_data: Iterable[DynamoItems] = (
            data[pos : pos + batch_size] for pos in range(0, len(data), batch_size)
        )
        with self.table.batch_writer() as writer:
            for batch in batched_data:
                for item in batch:
                    writer.put_item(Item=item)

    def delete(self, **kwargs: Any) -> DynamoItem:
        kwargs["query"]["ReturnValues"] = "ALL_OLD"
        result = self.table.delete_item(**kwargs["query"]).get("Attributes", {})
        super().publish("delete", result, **kwargs)
        return result

    def batch_delete(self, **kwargs: Any) -> None:
        batch_size: int = kwargs.get("batch_size", 25)
        if not isinstance(kwargs["data"], list):
            raise BatchItemException("Batched data must be contained within a list")
        batched_data: Iterable[DynamoItems] = (
            kwargs["data"][pos : pos + batch_size]
            for pos in range(0, len(kwargs["data"]), batch_size)
        )
        with self.table.batch_writer() as writer:
            for batch in batched_data:
                for item in batch:
                    writer.delete_item(Key=item)

    def update(self, **kwargs: Any) -> DynamoItem:
        original_data = self._get_original_data(**kwargs)
        merged_data = merge(original_data, kwargs["data"], **kwargs)
        updated_data = map_to_schema(merged_data, self.schema_file, self.schema)
        put_kwargs = {"Item": updated_data}
        if self.idempotence_key:
            original_value = original_data.get(self.idempotence_key)
            if original_value is None:
                raise ValueError(
                    f"idempotence key '{self.idempotence_key}' not found in original item"
                )
            put_kwargs["ConditionExpression"] = cast(Any, Attr(self.idempotence_key).eq(original_value))
        self.table.put_item(**put_kwargs)
        super().publish("update", updated_data, **kwargs)
        return updated_data

    def _get_original_data(self, **kwargs: Any) -> DynamoItem:
        if kwargs["operation"] == "get":
            original_data = self.get(**kwargs)
        else:
            query_result = self.query(**kwargs)
            if isinstance(query_result, list):
                items = query_result
            else:
                items = cast(DynamoItems, query_result.get("Items", []))
            if not items:
                raise ValueError("update: no data found to update")
            original_data = items[0]
        if not original_data:
            raise ValueError("update: no data found to update")
        return original_data
