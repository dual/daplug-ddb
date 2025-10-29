"""Type exports for daplug_ddb."""

from .dynamo_item import DynamoItem
from .dynamo_items import DynamoItems
from .message_attributes import MessageAttributes
from .prefix_config import PrefixConfig

__all__ = [
    "DynamoItem",
    "DynamoItems",
    "MessageAttributes",
    "PrefixConfig",
]
