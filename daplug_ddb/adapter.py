"""DynamoDB adapter exposing normalized CRUD operations."""

from copy import deepcopy
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, Iterable, Optional, Union

import boto3
from boto3.dynamodb.conditions import Attr

from daplug_ddb.prefixer import DynamodbPrefixer
from daplug_ddb.types import DynamoItem, DynamoItems

from .common import map_to_schema, merge
from .common.base_adapter import BaseAdapter
from .exception import BatchItemException


class DynamodbAdapter(BaseAdapter):
    """Implements DynamoDB CRUD operations with schema normalization."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.table = self.__get_dynamo_table(kwargs["table"], kwargs.get("endpoint"))
        self.schema_file: Optional[str] = kwargs.get("schema_file")
        self.identifier: str = kwargs.get("identifier", "")
        self.idempotence_key: Optional[str] = kwargs.get("idempotence_key")
        self.raise_idempotence_error: bool = kwargs.get("raise_idempotence_error", False)
        self.idempotence_use_latest: bool = kwargs.get("idempotence_use_latest", False)

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
        prefixer = self.__build_prefixer(kwargs)
        response = self.table.scan(**kwargs.get("query", {}))
        cleaned = prefixer.remove_prefix(response)
        if kwargs.get("raw_scan") and isinstance(cleaned, dict):
            return cleaned
        if isinstance(cleaned, dict):
            return cleaned.get("Items", [])
        return response if kwargs.get("raw_scan") else response.get("Items", [])

    def get(self, **kwargs: Any) -> DynamoItem:
        prefixer = self.__build_prefixer(kwargs)
        query = self.__prefixed_query(kwargs.get("query"), prefixer)
        result: Dict[str, Any] = self.table.get_item(**query)
        cleaned = prefixer.remove_prefix(result.get("Item", {}))
        return cleaned if isinstance(cleaned, dict) else result.get("Item", {})

    def query(self, **kwargs: Any) -> Union[DynamoItems, Dict[str, Any]]:
        prefixer = self.__build_prefixer(kwargs)
        prefixed_query = self.__prefixed_query(kwargs.get("query"), prefixer)
        response = self.table.query(**prefixed_query)
        cleaned = prefixer.remove_prefix(response)
        if kwargs.get("raw_query") and isinstance(cleaned, dict):
            return cleaned
        if isinstance(cleaned, dict):
            return cleaned.get("Items", [])
        return response if kwargs.get("raw_query") else response.get("Items", [])

    def overwrite(self, **kwargs: Any) -> DynamoItem:
        payload = self.__map_with_schema(kwargs["data"], kwargs)
        prefixer = self.__build_prefixer(kwargs)
        stored_item = prefixer.add_prefix(payload)
        if isinstance(stored_item, dict):
            self.table.put_item(Item=stored_item)
            cleaned = prefixer.remove_prefix(stored_item)
            if isinstance(cleaned, dict):
                super().publish("create", cleaned, **kwargs)
                return cleaned
            super().publish("create", payload, **kwargs)
            return payload
        self.table.put_item(Item=payload)
        super().publish("create", payload, **kwargs)
        return payload

    def insert(self, **kwargs: Any) -> DynamoItem:
        payload = self.__map_with_schema(kwargs["data"], kwargs)
        prefixer = self.__build_prefixer(kwargs)
        stored_item = prefixer.add_prefix(payload)
        if isinstance(stored_item, dict):
            self.table.put_item(
                Item=stored_item, ConditionExpression=Attr(self.identifier).not_exists()
            )
            cleaned = prefixer.remove_prefix(stored_item)
            if isinstance(cleaned, dict):
                super().publish("create", cleaned, **kwargs)
                return cleaned
            super().publish("create", payload, **kwargs)
            return payload
        self.table.put_item(
            Item=payload, ConditionExpression=Attr(self.identifier).not_exists()
        )
        super().publish("create", payload, **kwargs)
        return payload

    def batch_insert(self, **kwargs: Any) -> None:
        data = kwargs["data"]
        batch_size: int = kwargs.get("batch_size", 25)

        if not isinstance(data, list):
            raise BatchItemException("Batched data must be contained within a list")
        mapped_items = [self.__map_with_schema(item, kwargs) for item in data]
        prefixer = self.__build_prefixer(kwargs)
        batched_data: Iterable[DynamoItems] = (
            mapped_items[pos: pos + batch_size] for pos in range(0, len(mapped_items), batch_size)
        )
        with self.table.batch_writer() as writer:
            for batch in batched_data:
                prefixed_batch = prefixer.add_prefix(batch)
                items_to_store = prefixed_batch if isinstance(prefixed_batch, list) else batch
                for item in items_to_store:
                    writer.put_item(Item=item)

    def delete(self, **kwargs: Any) -> DynamoItem:
        prefixer = self.__build_prefixer(kwargs)
        query = self.__prefixed_query(kwargs.get("query"), prefixer)
        query["ReturnValues"] = "ALL_OLD"
        result = self.table.delete_item(**query).get("Attributes", {})
        cleaned = prefixer.remove_prefix(result)
        cleaned_item = cleaned if isinstance(cleaned, dict) else result
        super().publish("delete", cleaned_item, **kwargs)
        return cleaned_item if isinstance(cleaned_item, dict) else {}

    def batch_delete(self, **kwargs: Any) -> None:
        batch_size: int = kwargs.get("batch_size", 25)
        if not isinstance(kwargs["data"], list):
            raise BatchItemException("Batched data must be contained within a list")
        batched_data: Iterable[DynamoItems] = (
            kwargs["data"][pos: pos + batch_size]
            for pos in range(0, len(kwargs["data"]), batch_size)
        )
        prefixer = self.__build_prefixer(kwargs)
        with self.table.batch_writer() as writer:
            for batch in batched_data:
                prefixed_batch = prefixer.add_prefix(batch)
                items_to_delete = prefixed_batch if isinstance(prefixed_batch, list) else batch
                for item in items_to_delete:
                    writer.delete_item(Key=item)

    def update(self, **kwargs: Any) -> DynamoItem:
        prefixer = self.__build_prefixer(kwargs)
        original_data = self.__get_original_data(**kwargs)
        merged_data = merge(original_data, kwargs["data"], **kwargs)
        payload = self.__map_with_schema(merged_data, kwargs)
        stored_item = prefixer.add_prefix(payload)
        if isinstance(stored_item, dict):
            data_to_store = stored_item
            response_template = prefixer.remove_prefix(stored_item)
            if not isinstance(response_template, dict):
                response_template = payload
        else:
            data_to_store = payload
            response_template = payload
        original_value = (
            original_data.get(self.idempotence_key)  # type: ignore[arg-type]
            if isinstance(original_data, dict)
            else None
        )
        new_value = response_template.get(self.idempotence_key) if isinstance(response_template, dict) else None
        if self.__should_use_latest(original_value, new_value):
            return self.__clean_for_response(prefixer, original_data)
        put_kwargs = self.__build_put_kwargs(original_value, data_to_store)
        self.table.put_item(**put_kwargs)
        cleaned_item = self.__clean_for_response(prefixer, data_to_store)
        super().publish("update", cleaned_item, **kwargs)
        return cleaned_item

    @lru_cache(maxsize=128)
    def __get_dynamo_table(self, table: str, endpoint: Optional[str] = None) -> Any:
        return boto3.resource("dynamodb", endpoint_url=endpoint).Table(table)

    def __build_put_kwargs(self, original_value: Any, data_to_store: Dict[str, Any]) -> Dict[str, Any]:
        put_kwargs: Dict[str, Any] = {"Item": data_to_store}
        if not self.idempotence_key:
            return put_kwargs
        if original_value is None:
            if self.raise_idempotence_error:
                raise ValueError(
                    f"idempotence key '{self.idempotence_key}' not found in original item"
                )
            return put_kwargs
        if (
            original_value != data_to_store.get(self.idempotence_key)
            and self.raise_idempotence_error
        ):
            raise ValueError("update: idempotence key value has changed")
        put_kwargs["ConditionExpression"] = Attr(self.idempotence_key).eq(original_value)
        return put_kwargs

    def __clean_for_response(self, prefixer: DynamodbPrefixer, item: Dict[str, Any]) -> Dict[str, Any]:
        cleaned = prefixer.remove_prefix(item)
        return cleaned if isinstance(cleaned, dict) else item

    def __get_original_data(self, **kwargs: Any) -> DynamoItem:
        if kwargs["operation"] == "get":
            original_data = self.get(**kwargs)
        else:
            query_result = self.query(**kwargs)
            if isinstance(query_result, list):
                items = query_result
            elif isinstance(query_result, dict):
                items = query_result.get("Items", [])
            else:
                items = []
            if not items:
                raise ValueError("update: no data found to update")
            original_data = items[0]
        if not original_data:
            raise ValueError("update: no data found to update")
        return original_data

    def __prefixed_query(self, query: Optional[Dict[str, Any]], prefixer: DynamodbPrefixer) -> Dict[str, Any]:
        if not query:
            return {}
        prefixed = dict(query)
        key = prefixed.get("Key")
        if isinstance(key, dict):
            prefixed_key = prefixer.add_prefix(key)
            if isinstance(prefixed_key, dict):
                prefixed["Key"] = prefixed_key
        exclusive = prefixed.get("ExclusiveStartKey")
        if isinstance(exclusive, dict):
            prefixed_key = prefixer.add_prefix(exclusive)
            if isinstance(prefixed_key, dict):
                prefixed["ExclusiveStartKey"] = prefixed_key
        return prefixed

    def __should_use_latest(self, original_value: Any, new_value: Any) -> bool:
        if not self.idempotence_use_latest or not self.idempotence_key:
            return False
        if original_value is None or new_value is None:
            return False
        try:
            original_dt = datetime.fromisoformat(str(original_value))
            new_dt = datetime.fromisoformat(str(new_value))
        except ValueError as exc:
            raise ValueError("idempotence_use_latest requires ISO date-compatible values") from exc
        return original_dt > new_dt

    def __map_with_schema(self, data: Dict[str, Any], call_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        schema_file = call_kwargs.get("schema_file") or self.schema_file
        schema_name = call_kwargs.get("schema")
        if schema_file and schema_name:
            return map_to_schema(data, schema_file, schema_name)
        return deepcopy(data)

    def __build_prefixer(self, call_kwargs: Dict[str, Any]) -> DynamodbPrefixer:
        config: Dict[str, Any] = {}
        for key in ("hash_key", "hash_prefix", "range_key", "range_prefix"):
            value = call_kwargs.get(key)
            if value is not None:
                config[key] = value
        return DynamodbPrefixer(**config)
