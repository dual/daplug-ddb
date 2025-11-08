from typing import Any, Dict

from daplug_ddb.types import MessageAttributes

from . import publisher


class BaseAdapter:

    def __init__(self, **kwargs: Any) -> None:
        self.publisher = publisher
        self.sns_arn = kwargs.get("sns_arn")
        self.sns_endpoint = kwargs.get("sns_endpoint")
        self.sns_defaults = kwargs.get("sns_attributes", {})

    def publish(self, db_operation: str, db_data: Dict[str, Any], **kwargs: Any) -> None:
        attributes = self.create_format_attributes(db_operation, kwargs.get("sns_attributes", {}))
        self.publisher.publish(
            endpoint=self.sns_endpoint,
            arn=self.sns_arn,
            attributes=attributes,
            data=db_data,
            fifo_group_id=kwargs.get("fifo_group_id"),
            fifo_duplication_id=kwargs.get("fifo_duplication_id"),
        )

    def create_format_attributes(self, operation: str, call_attributes: dict) -> MessageAttributes:
        combined = {**self.sns_defaults, **call_attributes, **{"operation": operation}}
        formatted_attributes = {}
        for key, value in combined.items():
            if value is not None:
                data_type = "String" if isinstance(value, str) else "Number"
                formatted_attributes[key] = {
                    "DataType": data_type,
                    "StringValue": value,
                }
        return formatted_attributes
