"""Safe JSON encoding/decoding helpers."""

from typing import Any

import json


def try_decode_json(possible_json: Any) -> Any:
    try:
        return json.loads(possible_json)
    except Exception:  # pylint: disable=broad-except
        return possible_json


def try_encode_json(possible_json: Any) -> Any:
    try:
        return json.dumps(possible_json)
    except Exception:  # pylint: disable=broad-except
        return possible_json
