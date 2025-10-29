"""Type alias for a list of DynamoDB items."""

from typing import List

from .dynamo_item import DynamoItem

DynamoItems = List[DynamoItem]

__all__ = ["DynamoItems"]
