"""Typed configuration describing schema validation options."""

from typing import TypedDict


class SchemaConfig(TypedDict, total=False):
    schema: str
    schema_file: str


__all__ = ["SchemaConfig"]
