"""Public interface for the daplug_ddb package."""

from typing import Any

from .adapter import BatchItemException, DynamodbAdapter


def adapter(**kwargs: Any) -> DynamodbAdapter:
    """Factory helper mirroring legacy entry point for DynamoDB adapters."""

    engine = kwargs.pop("engine", "dynamodb")
    if engine and engine != "dynamodb":  # preserve deterministic erroring for unsupported engines
        raise ValueError(f"engine {engine} not supported; only 'dynamodb' is available")
    return DynamodbAdapter(**kwargs)


__all__ = ["adapter", "DynamodbAdapter", "BatchItemException"]
