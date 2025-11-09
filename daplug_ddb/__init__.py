from typing import Any

from .adapter import BatchItemException, DynamodbAdapter


def adapter(**kwargs: Any) -> DynamodbAdapter:
    """Factory helper for creating a DynamoDB adapter."""

    kwargs.pop("engine", None)  # allow legacy callers to pass engine without effect
    return DynamodbAdapter(**kwargs)


__all__ = ["adapter", "DynamodbAdapter", "BatchItemException"]
