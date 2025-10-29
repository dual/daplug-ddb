"""Unit tests for the DynamodbPrefixer helper."""

from daplug_ddb.prefixer import DynamodbPrefixer


def test_add_prefix_single_item() -> None:
    prefixer = DynamodbPrefixer(hash_key="pk", hash_prefix="tenant#", range_key="sk", range_prefix="type#")
    item = {"pk": "123", "sk": "abc", "name": "widget"}

    result = prefixer.add_prefix(item)

    assert isinstance(result, dict)
    assert result["pk"] == "tenant#123"
    assert result["sk"] == "type#abc"
    assert result["name"] == "widget"
    # original untouched
    assert item["pk"] == "123"


def test_remove_prefix_single_item() -> None:
    prefixer = DynamodbPrefixer(hash_key="pk", hash_prefix="tenant#", range_key="sk", range_prefix="type#")
    item = {"pk": "tenant#123", "sk": "type#abc", "name": "widget"}

    result = prefixer.remove_prefix(item)

    assert isinstance(result, dict)
    assert result["pk"] == "123"
    assert result["sk"] == "abc"
    assert result["name"] == "widget"


def test_add_prefix_list_of_items() -> None:
    prefixer = DynamodbPrefixer(hash_key="pk", hash_prefix="tenant#")
    items = [{"pk": "1"}, {"pk": "2"}]

    result = prefixer.add_prefix(items)

    assert isinstance(result, list)
    assert [i["pk"] for i in result] == ["tenant#1", "tenant#2"]
    assert [i["pk"] for i in items] == ["1", "2"]


def test_remove_prefix_from_response_dict() -> None:
    prefixer = DynamodbPrefixer(hash_key="pk", hash_prefix="tenant#")
    response = {
        "Items": [{"pk": "tenant#1"}, {"pk": "tenant#2"}],
        "LastEvaluatedKey": {"pk": "tenant#3"},
    }

    cleaned = prefixer.remove_prefix(response)

    assert isinstance(cleaned, dict)
    assert cleaned["Items"][0]["pk"] == "1"
    assert cleaned["LastEvaluatedKey"]["pk"] == "3"


def test_add_prefix_skips_missing_values() -> None:
    prefixer = DynamodbPrefixer(hash_key="pk", hash_prefix="tenant#")
    item = {"sk": "abc"}

    result = prefixer.add_prefix(item)

    assert isinstance(result, dict)
    assert result == item  # untouched
