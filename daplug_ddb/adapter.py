"""DynamoDB adapter exposing normalized CRUD operations."""

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
        self.table = self._get_dynamo_table(kwargs["table"], kwargs.get("endpoint"))
        self.schema_file: str = kwargs["schema_file"]
        self.schema: str = kwargs["schema"]
        self.identifier: str = kwargs["identifier"]
        self.idempotence_key: str = kwargs.get("idempotence_key", "")
        self.raise_idempotence_error: bool = kwargs.get("raise_idempotence_error", False)
        self.idempotence_use_latest: bool = kwargs.get("idempotence_use_latest", False)
        self.prefixer = DynamodbPrefixer(
            hash_key=kwargs.get("hash_key"),
            hash_prefix=kwargs.get("hash_prefix"),
            range_key=kwargs.get("range_key"),
            range_prefix=kwargs.get("range_prefix"),
        )

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
        response = self.table.scan(**kwargs.get("query", {}))
        cleaned = self.prefixer.remove_prefix(response)
        if kwargs.get("raw_scan") and isinstance(cleaned, dict):
            return cleaned
        if isinstance(cleaned, dict):
            return cleaned.get("Items", [])
        return response if kwargs.get("raw_scan") else response.get("Items", [])

    def get(self, **kwargs: Any) -> DynamoItem:
        query = self.__prefixed_query(kwargs.get("query"))
        result: Dict[str, Any] = self.table.get_item(**query)
        cleaned = self.prefixer.remove_prefix(result.get("Item", {}))
        return cleaned if isinstance(cleaned, dict) else result.get("Item", {})

    def query(self, **kwargs: Any) -> Union[DynamoItems, Dict[str, Any]]:
        prefixed_query = self.__prefixed_query(kwargs.get("query"))
        response = self.table.query(**prefixed_query)
        cleaned = self.prefixer.remove_prefix(response)
        if kwargs.get("raw_query") and isinstance(cleaned, dict):
            return cleaned
        if isinstance(cleaned, dict):
            return cleaned.get("Items", [])
        return response if kwargs.get("raw_query") else response.get("Items", [])

    def overwrite(self, **kwargs: Any) -> DynamoItem:
        overwrite_item = map_to_schema(kwargs["data"], self.schema_file, self.schema)
        stored_item = self.prefixer.add_prefix(overwrite_item)
        if isinstance(stored_item, dict):
            self.table.put_item(Item=stored_item)
            cleaned = self.prefixer.remove_prefix(stored_item)
            if isinstance(cleaned, dict):
                super().publish("create", cleaned, **kwargs)
                return cleaned
            super().publish("create", overwrite_item, **kwargs)
            return overwrite_item
        self.table.put_item(Item=overwrite_item)
        super().publish("create", overwrite_item, **kwargs)
        return overwrite_item

    def insert(self, **kwargs: Any) -> DynamoItem:
        new_item = map_to_schema(kwargs["data"], self.schema_file, self.schema)
        stored_item = self.prefixer.add_prefix(new_item)
        if isinstance(stored_item, dict):
            self.table.put_item(
                Item=stored_item, ConditionExpression=Attr(self.identifier).not_exists()
            )
            cleaned = self.prefixer.remove_prefix(stored_item)
            if isinstance(cleaned, dict):
                super().publish("create", cleaned, **kwargs)
                return cleaned
            super().publish("create", new_item, **kwargs)
            return new_item
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
            data[pos: pos + batch_size] for pos in range(0, len(data), batch_size)
        )
        with self.table.batch_writer() as writer:
            for batch in batched_data:
                prefixed_batch = self.prefixer.add_prefix(batch)
                items_to_store = prefixed_batch if isinstance(prefixed_batch, list) else batch
                for item in items_to_store:
                    writer.put_item(Item=item)

    def delete(self, **kwargs: Any) -> DynamoItem:
        query = self.__prefixed_query(kwargs.get("query"))
        query["ReturnValues"] = "ALL_OLD"
        result = self.table.delete_item(**query).get("Attributes", {})
        cleaned = self.prefixer.remove_prefix(result)
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
        with self.table.batch_writer() as writer:
            for batch in batched_data:
                prefixed_batch = self.prefixer.add_prefix(batch)
                items_to_delete = prefixed_batch if isinstance(prefixed_batch, list) else batch
                for item in items_to_delete:
                    writer.delete_item(Key=item)

    def update(self, **kwargs: Any) -> DynamoItem:
        original_data = self.__get_original_data(**kwargs)
        data_to_store, response_template = self.__prepare_update_payload(original_data, kwargs)
        if self.__should_use_latest(
            original_data.get(self.idempotence_key), response_template.get(self.idempotence_key)
        ):
            return self.__clean_for_response(original_data)
        put_kwargs = self.__build_put_kwargs(original_data.get(self.idempotence_key), data_to_store)
        self.table.put_item(**put_kwargs)
        cleaned_item = self.__clean_for_response(data_to_store)
        super().publish("update", cleaned_item, **kwargs)
        return cleaned_item

    def __prepare_update_payload(
        self, original_data: DynamoItem, update_kwargs: Dict[str, Any]
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        merged_data = merge(original_data, update_kwargs["data"], **update_kwargs)
        updated_data = map_to_schema(merged_data, self.schema_file, self.schema)
        stored_item = self.prefixer.add_prefix(updated_data)
        if isinstance(stored_item, dict):
            return stored_item, self.__clean_for_response(stored_item)
        return updated_data, updated_data

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

    def __clean_for_response(self, item: Dict[str, Any]) -> Dict[str, Any]:
        cleaned = self.prefixer.remove_prefix(item)
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

    def __prefixed_query(self, query: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not query:
            return {}
        prefixed = dict(query)
        key = prefixed.get("Key")
        if isinstance(key, dict):
            prefixed_key = self.prefixer.add_prefix(key)
            if isinstance(prefixed_key, dict):
                prefixed["Key"] = prefixed_key
        exclusive = prefixed.get("ExclusiveStartKey")
        if isinstance(exclusive, dict):
            prefixed_key = self.prefixer.add_prefix(exclusive)
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
