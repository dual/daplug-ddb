"""Typed configuration for DynamoDB key prefixing."""

from typing import TypedDict


class PrefixConfig(TypedDict, total=False):
    hash_key: str
    hash_prefix: str
    range_key: str
    range_prefix: str
