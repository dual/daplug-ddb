"""Loads schema definitions with JSON reference resolution."""

from typing import Any, Dict

import jsonref
import simplejson as json
import yaml


def load_schema(schema_file: str, schema_key: str) -> Dict[str, Any]:
    with open(schema_file, encoding="UTF-8") as openapi:
        api_doc = yaml.load(openapi, Loader=yaml.FullLoader)
    return jsonref.loads(json.dumps(api_doc))["components"]["schemas"][schema_key]
