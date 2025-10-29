"""Lightweight stdout logger honoring RUN_MODE."""

from typing import Any, Dict

import os

from . import json_helper


def log(**kwargs: Any) -> None:
    if os.getenv("RUN_MODE") != "unittest":
        payload: Dict[str, Any] = {
            "level": kwargs.get("level", "INFO"),
            "log": kwargs.get("log", {}),
        }
        print(json_helper.try_encode_json(payload))
