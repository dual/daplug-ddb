from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Optional

from daplug_ddb.types import DynamoItem


class DynamodbPrefixer:

    def __init__(self, **kwargs: Any) -> None:
        self.hash_key: Optional[str] = kwargs.get("hash_key")
        self.hash_prefix: Optional[str] = kwargs.get("hash_prefix")
        self.range_key: Optional[str] = kwargs.get("range_key")
        self.range_prefix: Optional[str] = kwargs.get("range_prefix")

    def add_prefix(self, data: Any) -> Any:
        if data is None:
            return None
        return self.__process(data, add=True)

    def remove_prefix(self, data: Any) -> Any:
        if data is None:
            return None
        return self.__process(data, add=False)

    def __process(self, data: Any, add: bool) -> Any:
        if isinstance(data, list):
            return [self.__process_item(item, add) if isinstance(item, dict) else item for item in data]
        if isinstance(data, dict):
            processed: Dict[str, Any] = deepcopy(data)
            if "Items" in processed and isinstance(processed["Items"], list):
                processed["Items"] = [self.__process_item(item, add) if isinstance(
                    item, dict) else item for item in processed["Items"]]
            if "Item" in processed and isinstance(processed["Item"], dict):
                processed["Item"] = self.__process_item(processed["Item"], add)
            if "LastEvaluatedKey" in processed and isinstance(processed["LastEvaluatedKey"], dict):
                processed["LastEvaluatedKey"] = self.__process_item(processed["LastEvaluatedKey"], add)
            if "Key" in processed and isinstance(processed["Key"], dict):
                processed["Key"] = self.__process_item(processed["Key"], add)
            if not any(k in processed for k in ("Items", "Item", "LastEvaluatedKey", "Key")):
                return self.__process_item(processed, add)
            return processed
        return data

    def __process_item(self, item: DynamoItem, add: bool) -> DynamoItem:
        processed: DynamoItem = deepcopy(item)
        self.__apply_prefix(processed, self.hash_key, self.hash_prefix, add)
        self.__apply_prefix(processed, self.range_key, self.range_prefix, add)
        return processed

    def __apply_prefix(self, item: DynamoItem, key_name: Optional[str], prefix: Optional[str], add: bool) -> None:
        if not key_name or not prefix:
            return
        value = item.get(key_name)
        if not isinstance(value, str):
            return
        if add:
            if not value.startswith(prefix):
                item[key_name] = f"{prefix}{value}"
        else:
            if value.startswith(prefix):
                item[key_name] = value[len(prefix):]
