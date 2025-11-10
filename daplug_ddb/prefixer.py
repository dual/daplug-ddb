from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional, Sequence

from daplug_ddb.types import DynamoItem, TransformRule


class DynamodbPrefixer:
    """Adds or removes configured prefixes across DynamoDB request/response shapes."""

    def __init__(self, **kwargs: Any) -> None:
        self.hash_key: Optional[str] = kwargs.get("hash_key")
        self.hash_prefix: Optional[str] = kwargs.get("hash_prefix")
        self.range_key: Optional[str] = kwargs.get("range_key")
        self.range_prefix: Optional[str] = kwargs.get("range_prefix")
        self.enabled: bool = any(
            [
                self.hash_key and self.hash_prefix,
                self.range_key and self.range_prefix,
            ]
        )

    # ------------------------------------------------------------------
    # Public helpers ----------------------------------------------------
    def apply_item(self, item: Optional[DynamoItem], *, add: bool) -> Optional[DynamoItem]:
        if not self.enabled or not isinstance(item, dict):
            return item
        processed = deepcopy(item)
        self.__apply_prefix(processed, self.hash_key, self.hash_prefix, add)
        self.__apply_prefix(processed, self.range_key, self.range_prefix, add)
        return processed

    def apply_items(self, items: Optional[Iterable[DynamoItem]], *, add: bool) -> Optional[List[DynamoItem]]:
        if items is None:
            return None
        materialized = list(items)
        if not self.enabled:
            return materialized
        processed: List[DynamoItem] = []
        for entry in materialized:
            if not isinstance(entry, dict):
                processed.append(entry)
                continue
            updated = self.apply_item(entry, add=add)
            processed.append(updated if isinstance(updated, dict) else entry)
        return processed

    def apply_key(self, key: Optional[Dict[str, Any]], *, add: bool) -> Optional[Dict[str, Any]]:
        return self.apply_item(key, add=add)

    def apply_request(self, params: Optional[Dict[str, Any]], *, add: bool) -> Optional[Dict[str, Any]]:
        if not self.enabled or not isinstance(params, dict):
            return params
        updated = deepcopy(params)
        if isinstance(updated.get("Key"), dict):
            key_value = self.apply_key(updated["Key"], add=add)
            if isinstance(key_value, dict):
                updated["Key"] = key_value
        if isinstance(updated.get("ExclusiveStartKey"), dict):
            exclusive_value = self.apply_key(updated["ExclusiveStartKey"], add=add)
            if isinstance(exclusive_value, dict):
                updated["ExclusiveStartKey"] = exclusive_value
        if isinstance(updated.get("ExpressionAttributeValues"), dict):
            updated["ExpressionAttributeValues"] = self.__apply_expression_values(
                updated["ExpressionAttributeValues"],
                updated.get("ExpressionAttributeNames"),
                add,
            )
        return updated

    def apply_response(self, payload: Optional[Dict[str, Any]], *, add: bool) -> Optional[Dict[str, Any]]:
        if not self.enabled or not isinstance(payload, dict):
            return payload
        updated = deepcopy(payload)
        rules: Sequence[TransformRule] = (
            ("Items", list, self.__transform_items),
            ("Item", dict, self.__transform_item),
            ("LastEvaluatedKey", dict, self.__transform_key),
            ("Attributes", dict, self.__transform_item),
            ("Key", dict, self.__transform_key),
        )
        self.__apply_response_rules(updated, rules, add)
        return updated

    # ------------------------------------------------------------------
    # Backwards compatibility wrappers ---------------------------------
    def add_prefix(self, data: Any) -> Any:
        return self.__compat_apply(data, add=True)

    def remove_prefix(self, data: Any) -> Any:
        return self.__compat_apply(data, add=False)

    # ------------------------------------------------------------------
    # Internal helpers --------------------------------------------------
    def __compat_apply(self, data: Any, add: bool) -> Any:
        if not self.enabled:
            return data
        if isinstance(data, dict):
            if any(
                key in data
                for key in ("Items", "Item", "LastEvaluatedKey", "Attributes", "Key")
            ):
                return self.apply_response(data, add=add) or data
            return self.apply_item(data, add=add) or data
        if isinstance(data, list):
            return self.apply_items(data, add=add)
        return data

    def __apply_prefix(self, item: DynamoItem, key_name: Optional[str], prefix: Optional[str], add: bool) -> None:
        if not key_name or not prefix:
            return
        value = item.get(key_name)
        if isinstance(value, str):
            item[key_name] = self.__apply_to_string(value, prefix, add)

    def __apply_expression_values(
        self,
        values: Dict[str, Any],
        names: Optional[Dict[str, str]],
        add: bool,
    ) -> Dict[str, Any]:
        processed: Dict[str, Any] = deepcopy(values)
        for placeholder, raw_value in processed.items():
            attribute = self.__resolve_attribute_name(placeholder, names)
            prefix = self.__prefix_for_attribute(attribute)
            if not prefix:
                continue
            processed[placeholder] = self.__apply_expression_value(raw_value, prefix, add)
        return processed

    def __resolve_attribute_name(
        self,
        placeholder: str,
        names: Optional[Dict[str, str]],
    ) -> Optional[str]:
        token = placeholder.lstrip(":")
        if token in (self.hash_key, self.range_key):
            return token
        if names:
            alias = f"#{token}"
            mapped = names.get(alias) or names.get(token)
            if mapped in (self.hash_key, self.range_key):
                return mapped
        return None

    def __prefix_for_attribute(self, attribute: Optional[str]) -> Optional[str]:
        if attribute is None:
            return None
        if attribute == self.hash_key:
            return self.hash_prefix
        if attribute == self.range_key:
            return self.range_prefix
        return None

    def __apply_expression_value(self, raw_value: Any, prefix: str, add: bool) -> Any:
        if isinstance(raw_value, str):
            return self.__apply_to_string(raw_value, prefix, add)
        if isinstance(raw_value, dict) and "S" in raw_value:
            new_value = self.__apply_to_string(raw_value["S"], prefix, add)
            updated = dict(raw_value)
            updated["S"] = new_value
            return updated
        return raw_value

    def __apply_to_string(self, value: str, prefix: str, add: bool) -> str:
        if add:
            return value if value.startswith(prefix) else f"{prefix}{value}"
        if value.startswith(prefix):
            return value[len(prefix):]
        return value

    def __apply_response_rules(self, payload: Dict[str, Any], rules: Sequence[TransformRule], add: bool,) -> None:
        for field, expected_type, transformer in rules:
            value = payload.get(field)
            if isinstance(value, expected_type):
                transformed = transformer(value, add)
                if isinstance(transformed, expected_type):
                    payload[field] = transformed

    def __transform_items(self, value: Any, add: bool) -> Optional[Any]:
        return self.apply_items(value, add=add)

    def __transform_item(self, value: Any, add: bool) -> Optional[Any]:
        return self.apply_item(value, add=add)

    def __transform_key(self, value: Any, add: bool) -> Optional[Any]:
        return self.apply_key(value, add=add)
