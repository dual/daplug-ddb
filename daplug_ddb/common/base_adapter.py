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
            "schema": kwargs.get("schema"),
            "identifier": kwargs.get("identifier"),
            "idempotence_key": kwargs.get("idempotence_key"),
            "author_identifier": kwargs.get("author_identifier"),
        }

    def publish(self, db_operation: str, db_data: Dict[str, Any], **kwargs: Any) -> None:
        attributes = self.create_format_attibutes(db_operation)
        self.publisher.publish(
            endpoint=self.sns_endpoint,
            arn=self.sns_arn,
            attributes=attributes,
            data=db_data,
            fifo_group_id=kwargs.get("fifo_group_id"),
            fifo_duplication_id=kwargs.get("fifo_duplication_id"),
        )

    def create_format_attibutes(self, operation: str) -> MessageAttributes:
        self.default_attributes["operation"] = operation
        custom_attributes = self.get_attributes()
        formatted_attributes: MessageAttributes = {}
        for key, value in custom_attributes.items():
            if value is not None:
                data_type = "String" if isinstance(value, str) else "Number"
                formatted_attributes[key] = {
                    "DataType": data_type,
                    "StringValue": value,
                }
        return formatted_attributes

    def get_attributes(self) -> Dict[str, Any]:
        if self.sns_defaults and self.sns_custom:
            return {**self.default_attributes, **self.sns_custom}
        if not self.sns_defaults and self.sns_custom:
            return self.sns_custom
        if self.sns_defaults and not self.sns_custom:
            return self.default_attributes
        return {}
