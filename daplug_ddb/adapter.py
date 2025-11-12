from copy import deepcopy
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, Iterable, Optional, Union

import boto3
from boto3.dynamodb.conditions import Attr

from daplug_core.schema_mapper import map_to_schema
from daplug_core.dict_merger import merge
from daplug_core.base_adapter import BaseAdapter

from daplug_ddb.prefixer import DynamodbPrefixer
from daplug_ddb.types import DynamoItem, DynamoItems
from .exception import BatchItemException


class DynamodbAdapter(BaseAdapter):

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.table = self.__get_dynamo_table(kwargs['table'], kwargs.get('endpoint'))
        self.schema_file: Optional[str] = kwargs.get('schema_file')
        self.hash_key: Optional[str] = kwargs.get('hash_key')
        self.idempotence_key: Optional[str] = kwargs.get('idempotence_key')
        self.raise_idempotence_error: bool = kwargs.get('raise_idempotence_error', False)
        self.idempotence_use_latest: bool = kwargs.get('idempotence_use_latest', False)
        self._prefix_keys = ('hash_key', 'hash_prefix', 'range_key', 'range_prefix')
        self._default_prefix_config = self.__extract_prefix_config(kwargs)

    @property
    def prefixing_enabled(self) -> bool:
        return bool(self._default_prefix_config)

    def create(self, **kwargs: Any) -> DynamoItem:
        if kwargs.get('operation') == 'overwrite':
            return self.overwrite(**kwargs)
        return self.insert(**kwargs)

    def read(self, **kwargs: Any) -> Union[DynamoItem, DynamoItems, Dict[str, Any]]:
        if kwargs.get('operation') == 'query':
            return self.query(**kwargs)
        if kwargs.get('operation') == 'scan':
            return self.scan(**kwargs)
        return self.get(**kwargs)

    def scan(self, **kwargs: Any) -> Union[DynamoItems, Dict[str, Any]]:
        prefixer = self.__build_prefixer(kwargs)
        request_args = self.__prepare_request_arguments(prefixer, kwargs)
        response = self.table.scan(**request_args)
        if not prefixer.enabled:
            return response if kwargs.get('raw_scan') else response.get('Items', [])
        cleaned_response = prefixer.apply_response(response, add=False)
        if kwargs.get('raw_scan'):
            return cleaned_response if cleaned_response is not None else response
        cleaned = cleaned_response or {}
        return cleaned.get('Items', [])

    def get(self, **kwargs: Any) -> DynamoItem:
        prefixer = self.__build_prefixer(kwargs)
        request_args = self.__prepare_request_arguments(prefixer, kwargs)
        result: Dict[str, Any] = self.table.get_item(**request_args)
        item = result.get('Item', {})
        if not prefixer.enabled:
            return item if isinstance(item, dict) else {}
        cleaned = prefixer.apply_item(item, add=False)
        return cleaned if isinstance(cleaned, dict) else item

    def query(self, **kwargs: Any) -> Union[DynamoItems, Dict[str, Any]]:
        prefixer = self.__build_prefixer(kwargs)
        request_args = self.__prepare_request_arguments(prefixer, kwargs)
        response = self.table.query(**request_args)
        if not prefixer.enabled:
            return response if kwargs.get('raw_query') else response.get('Items', [])
        cleaned_response = prefixer.apply_response(response, add=False)
        if kwargs.get('raw_query'):
            return cleaned_response if cleaned_response is not None else response
        cleaned = cleaned_response or {}
        return cleaned.get('Items', [])

    def overwrite(self, **kwargs: Any) -> DynamoItem:
        payload = self.__map_with_schema(kwargs['data'], kwargs)
        prefixer = self.__build_prefixer(kwargs)
        item_to_store = (
            prefixer.apply_item(payload, add=True)
            if prefixer.enabled
            else payload
        )
        self.table.put_item(Item=item_to_store)
        response_item = (
            prefixer.apply_item(item_to_store, add=False)
            if prefixer.enabled
            else payload
        )
        result_item = response_item if isinstance(response_item, dict) else payload
        super().publish(result_item, **kwargs)
        return result_item

    def insert(self, **kwargs: Any) -> DynamoItem:
        payload = self.__map_with_schema(kwargs['data'], kwargs)
        prefixer = self.__build_prefixer(kwargs)
        item_to_store = (
            prefixer.apply_item(payload, add=True)
            if prefixer.enabled
            else payload
        )
        if not self.hash_key:
            raise ValueError('insert requires hash_key to be configured')
        self.table.put_item(
            Item=item_to_store,
            ConditionExpression=Attr(self.hash_key).not_exists(),
        )
        response_item = (
            prefixer.apply_item(item_to_store, add=False)
            if prefixer.enabled
            else payload
        )
        result_item = response_item if isinstance(response_item, dict) else payload
        super().publish(result_item, **kwargs)
        return result_item

    def batch_insert(self, **kwargs: Any) -> None:
        data = kwargs['data']
        batch_size: int = kwargs.get('batch_size', 25)

        if not isinstance(data, list):
            raise BatchItemException('Batched data must be contained within a list')
        mapped_items = [self.__map_with_schema(item, kwargs) for item in data]
        prefixer = self.__build_prefixer(kwargs)
        batched_data: Iterable[DynamoItems] = (
            mapped_items[pos: pos + batch_size] for pos in range(0, len(mapped_items), batch_size)
        )
        with self.table.batch_writer() as writer:
            for batch in batched_data:
                if prefixer.enabled:
                    prefixed_batch = prefixer.apply_items(batch, add=True)
                    items_to_store = prefixed_batch if prefixed_batch is not None else batch
                else:
                    items_to_store = batch
                for item in items_to_store:
                    writer.put_item(Item=item)

    def delete(self, **kwargs: Any) -> DynamoItem:
        prefixer = self.__build_prefixer(kwargs)
        request_args = self.__prepare_request_arguments(prefixer, kwargs)
        request_args['ReturnValues'] = 'ALL_OLD'
        result = self.table.delete_item(**request_args).get('Attributes', {})
        if prefixer.enabled:
            cleaned = prefixer.apply_item(result, add=False)
            cleaned_item = cleaned if isinstance(cleaned, dict) else result
        else:
            cleaned_item = result
        super().publish(cleaned_item, **kwargs)
        return cleaned_item if isinstance(cleaned_item, dict) else {}

    def batch_delete(self, **kwargs: Any) -> None:
        batch_size: int = kwargs.get('batch_size', 25)
        if not isinstance(kwargs['data'], list):
            raise BatchItemException('Batched data must be contained within a list')
        batched_data: Iterable[DynamoItems] = (
            kwargs['data'][pos: pos + batch_size]
            for pos in range(0, len(kwargs['data']), batch_size)
        )
        prefixer = self.__build_prefixer(kwargs)
        with self.table.batch_writer() as writer:
            for batch in batched_data:
                if prefixer.enabled:
                    prefixed_batch = prefixer.apply_items(batch, add=True)
                    items_to_delete = prefixed_batch if prefixed_batch is not None else batch
                else:
                    items_to_delete = batch
                for item in items_to_delete:
                    writer.delete_item(Key=item)

    def update(self, **kwargs: Any) -> DynamoItem:
        prefixer = self.__build_prefixer(kwargs)
        original_data = self.__get_original_data(**kwargs)
        merged_data = merge(original_data, kwargs['data'], **kwargs)
        payload = self.__map_with_schema(merged_data, kwargs)
        if prefixer.enabled:
            prefixed_item = prefixer.apply_item(payload, add=True)
            data_to_store = prefixed_item if isinstance(prefixed_item, dict) else payload
            response_template_raw = prefixer.apply_item(data_to_store, add=False)
            response_template = (
                response_template_raw if isinstance(response_template_raw, dict) else payload
            )
        else:
            data_to_store = payload
            response_template = payload
        key_name = self.idempotence_key
        original_value = (
            original_data.get(key_name)
            if isinstance(original_data, dict) and key_name
            else None
        )
        new_value = (
            response_template.get(key_name)
            if isinstance(response_template, dict) and key_name
            else None
        )
        if self.__should_use_latest(original_value, new_value):
            return self.__clean_for_response(prefixer, original_data)
        put_kwargs = self.__build_put_kwargs(original_value, data_to_store)
        self.table.put_item(**put_kwargs)
        cleaned_item = self.__clean_for_response(prefixer, data_to_store)
        super().publish(cleaned_item, **kwargs)
        return cleaned_item

    @lru_cache(maxsize=128)
    def __get_dynamo_table(self, table: str, endpoint: Optional[str] = None) -> Any:
        return boto3.resource('dynamodb', endpoint_url=endpoint).Table(table)

    def __build_put_kwargs(self, original_value: Any, data_to_store: Dict[str, Any]) -> Dict[str, Any]:
        put_kwargs: Dict[str, Any] = {'Item': data_to_store}
        if not self.idempotence_key:
            return put_kwargs
        if original_value is None:
            if self.raise_idempotence_error:
                raise ValueError(f'idempotence key {self.idempotence_key} not found in original item')
            return put_kwargs
        if (
            original_value != data_to_store.get(self.idempotence_key)
            and self.raise_idempotence_error
        ):
            raise ValueError('update: idempotence key value has changed')
        put_kwargs['ConditionExpression'] = Attr(self.idempotence_key).eq(original_value)
        return put_kwargs

    def __clean_for_response(self, prefixer: DynamodbPrefixer, item: Dict[str, Any]) -> Dict[str, Any]:
        if not prefixer.enabled:
            return item
        cleaned = prefixer.apply_item(item, add=False)
        return cleaned if isinstance(cleaned, dict) else item

    def __get_original_data(self, **kwargs: Any) -> DynamoItem:
        if kwargs.get('operation') == 'get' or 'key' in kwargs.get('query', {}):
            original_data = self.get(**kwargs)
        else:
            query_result = self.query(**kwargs)
            if isinstance(query_result, list):
                items = query_result
            elif isinstance(query_result, dict):
                items = query_result.get('Items', [])
            else:
                items = []
            if not items:
                raise ValueError('update: no data found to update')
            original_data = items[0]
        if not original_data:
            raise ValueError('update: no data found to update')
        return original_data

    def __should_use_latest(self, original_value: Any, new_value: Any) -> bool:
        if not self.idempotence_use_latest or not self.idempotence_key:
            return False
        if original_value is None or new_value is None:
            return False
        try:
            original_dt = datetime.fromisoformat(str(original_value))
            new_dt = datetime.fromisoformat(str(new_value))
        except ValueError as exc:
            raise ValueError('idempotence_use_latest requires ISO date-compatible values') from exc
        return original_dt > new_dt

    def __map_with_schema(self, data: Dict[str, Any], call_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        schema_file = call_kwargs.get('schema_file') or self.schema_file
        schema_name = call_kwargs.get('schema')
        if schema_file and schema_name:
            return map_to_schema(data, schema_file, schema_name)
        return deepcopy(data)

    def __build_prefixer(self, call_kwargs: Dict[str, Any]) -> DynamodbPrefixer:
        config = dict(self._default_prefix_config)
        for key in self._prefix_keys:
            value = call_kwargs.get(key)
            if value is not None:
                config[key] = value
        return DynamodbPrefixer(**config)

    def __extract_prefix_config(self, source: Dict[str, Any]) -> Dict[str, Any]:
        config: Dict[str, Any] = {}
        for key in ('hash_key', 'hash_prefix', 'range_key', 'range_prefix'):
            value = source.get(key)
            if value is not None:
                config[key] = value
        return config

    def __prepare_request_arguments(self, prefixer: DynamodbPrefixer, call_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(call_kwargs.get('query'), dict):
            return {}
        if prefixer.enabled:
            transformed = prefixer.apply_request(call_kwargs['query'], add=True)
            return transformed if isinstance(transformed, dict) else {}
        return deepcopy(call_kwargs['query'])
