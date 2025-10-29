"""Type alias for SNS message attributes."""

from typing import Any, Dict

MessageAttributes = Dict[str, Dict[str, Any]]

__all__ = ["MessageAttributes"]
