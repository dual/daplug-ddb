"""Shared base adapter that wraps SNS publishing."""

from typing import Any, Dict, Optional

from daplug_ddb.types import MessageAttributes

from . import publisher


class BaseAdapter:
    """Provides shared publish helper logic for adapters."""

    def __init__(self, **kwargs: Any) -> None:
        self.sns_arn: Optional[str] = kwargs.get("sns_arn")
        self.sns_custom: Dict[str, Any] = kwargs.get("sns_attributes", {})
        self.sns_defaults: bool = kwargs.get("sns_default_attributes", True)
        self.sns_endpoint: Optional[str] = kwargs.get("sns_endpoint")
        self.publisher = publisher
        self.default_attributes: Dict[str, Any] = {
            "hash_key": kwargs.get("hash_key"),
            "idempotence_key": kwargs.get("idempotence_key"),
            "author_identifier": kwargs.get("author_identifier"),
        }

    def publish(self, db_operation: str, db_data: Dict[str, Any], **kwargs: Any) -> None:
        call_attributes: Dict[str, Any] = dict(kwargs.get("sns_attributes", {}))
        schema_name = kwargs.get("schema")
        if schema_name and "schema" not in call_attributes:
            call_attributes["schema"] = schema_name
        attributes = self.create_format_attibutes(db_operation, call_attributes)
        self.publisher.publish(
            endpoint=self.sns_endpoint,
            arn=self.sns_arn,
            attributes=attributes,
            data=db_data,
            fifo_group_id=kwargs.get("fifo_group_id"),
            fifo_duplication_id=kwargs.get("fifo_duplication_id"),
        )

    def create_format_attibutes(self, operation: str, call_attributes: dict) -> MessageAttributes:
        combined = self.__combined_attributes(operation, call_attributes)
        formatted_attributes: MessageAttributes = {}
        for key, value in combined.items():
            if value is not None:
                data_type = "String" if isinstance(value, str) else "Number"
                formatted_attributes[key] = {
                    "DataType": data_type,
                    "StringValue": value,
                }
        return formatted_attributes

    def __combined_attributes(self, operation: str, call_attributes: dict) -> Dict[str, Any]:
        pieces = []
        base: Dict[str, Any] = {}
        if self.sns_defaults:
            base.update(self.default_attributes)
        base["operation"] = operation
        pieces.append(base)

        if self.sns_custom:
            pieces.append(self.sns_custom)
        if call_attributes:
            pieces.append(call_attributes)

        return self.__merge_attributes(*pieces)

    def __merge_attributes(self, *dicts: Dict[str, Any]) -> Dict[str, Any]:
        merged: Dict[str, Any] = {}
        for data in dicts:
            if not data:
                continue
            merged.update(data)
        return merged
