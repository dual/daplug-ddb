"""Helpers that map arbitrary data to JSON schema-defined structures."""

from typing import Any, Dict, List

from daplug_ddb.types import DynamoItem

from . import schema_loader


def map_to_schema(data: DynamoItem, schema_file: str, schema_key: str) -> DynamoItem:
    model_data: DynamoItem = {}
    model_schema = schema_loader.load_schema(schema_file, schema_key)
    schemas = model_schema["allOf"] if model_schema.get("allOf") else [model_schema]
    for model in schemas:
        if model.get("type") == "object":
            _populate_model_data(model.get("properties", {}), data, model_data)
    return model_data


def _populate_model_data(
    properties: Dict[str, Any], data: Any, model_data: DynamoItem
) -> DynamoItem:
    if data and isinstance(data, dict):
        _populate_model_dict(properties, data, model_data)
    return model_data


def _populate_model_dict(properties: Dict[str, Any], data: Dict[str, Any], model_data: DynamoItem) -> None:
    for property_key, property_value in properties.items():
        model_data[property_key] = {}
        if property_value.get("properties"):
            _populate_model_data(
                property_value["properties"], data.get(property_key), model_data[property_key]
            )
        elif property_value.get("items", {}).get("properties"):
            _populate_model_list(model_data, property_key, property_value, data)
        else:
            model_data[property_key] = data.get(property_key)


def _populate_model_list(
    model_data: DynamoItem, property_key: str, property_value: Dict[str, Any], data: Dict[str, Any]
) -> None:
    model_data[property_key] = []
    items: List[Any] = data.get(property_key, [])
    for index in range(len(items)):  # pylint: disable=consider-using-enumerate
        if data.get(property_key) and isinstance(items, list) and index < len(items):
            pop = _populate_model_data(
                property_value["items"]["properties"], items[index], {}
            )
            model_data[property_key].append(pop)
