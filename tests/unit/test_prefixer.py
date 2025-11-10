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


def test_apply_item_returns_original_when_disabled() -> None:
    prefixer = DynamodbPrefixer(hash_key="pk")
    item = {"pk": "123"}

    result = prefixer.apply_item(item, add=True)

    assert result is item


def test_apply_items_handles_mixed_entries() -> None:
    prefixer = DynamodbPrefixer(hash_key="pk", hash_prefix="tenant#")
    items = [{"pk": "1"}, "not-a-dict"]

    result = prefixer.apply_items(items, add=True)

    assert isinstance(result, list)
    assert result[0]["pk"] == "tenant#1"
    assert result[1] == "not-a-dict"


def test_apply_request_with_aliases_and_expression_dicts() -> None:
    prefixer = DynamodbPrefixer(
        hash_key="pk",
        hash_prefix="tenant#",
        range_key="sk",
        range_prefix="type#",
    )
    params = {
        "Key": {"pk": "1", "sk": "a"},
        "ExclusiveStartKey": {"pk": "2", "sk": "b"},
        "ExpressionAttributeNames": {"#id": "pk", "#rk": "sk"},
        "ExpressionAttributeValues": {
            ":id": "1",
            ":rk": {"S": "a"},
        },
    }

    updated = prefixer.apply_request(params, add=True)

    assert updated["Key"]["pk"] == "tenant#1"
    assert updated["ExclusiveStartKey"]["pk"] == "tenant#2"
    assert updated["ExpressionAttributeValues"][":id"] == "tenant#1"
    assert updated["ExpressionAttributeValues"][":rk"]["S"] == "type#a"


def test_apply_response_transforms_all_sections() -> None:
    prefixer = DynamodbPrefixer(
        hash_key="pk",
        hash_prefix="tenant#",
        range_key="sk",
        range_prefix="type#",
    )
    payload = {
        "Items": [{"pk": "tenant#1", "sk": "type#a"}],
        "Item": {"pk": "tenant#2", "sk": "type#b"},
        "LastEvaluatedKey": {"pk": "tenant#3", "sk": "type#c"},
        "Attributes": {"pk": "tenant#4", "sk": "type#d"},
        "Key": {"pk": "tenant#5", "sk": "type#e"},
    }

    cleaned = prefixer.apply_response(payload, add=False)

    assert cleaned["Items"][0]["pk"] == "1"
    assert cleaned["Item"]["sk"] == "b"
    assert cleaned["LastEvaluatedKey"]["pk"] == "3"
    assert cleaned["Attributes"]["sk"] == "d"
    assert cleaned["Key"]["pk"] == "5"


def test_compat_apply_handles_plain_containers() -> None:
    prefixer = DynamodbPrefixer(hash_key="pk", hash_prefix="tenant#")

    dict_result = prefixer.add_prefix({"pk": "1"})
    list_result = prefixer.add_prefix([{"pk": "2"}])

    assert dict_result["pk"] == "tenant#1"
    assert list_result[0]["pk"] == "tenant#2"


def test_apply_request_returns_same_value_for_non_dict() -> None:
    prefixer = DynamodbPrefixer(hash_key="pk", hash_prefix="tenant#")

    assert prefixer.apply_request(None, add=True) is None


def test_apply_items_returns_none_when_input_none() -> None:
    prefixer = DynamodbPrefixer(hash_key="pk", hash_prefix="tenant#")

    assert prefixer.apply_items(None, add=True) is None


def test_apply_items_returns_materialized_when_disabled() -> None:
    prefixer = DynamodbPrefixer(hash_key="pk")
    items = [{"pk": "1"}]

    assert prefixer.apply_items(items, add=True) == items


def test_apply_response_returns_payload_when_disabled() -> None:
    prefixer = DynamodbPrefixer(hash_key="pk")
    payload = {"Items": [{"pk": "1"}]}

    assert prefixer.apply_response(payload, add=False) == payload


def test_compat_apply_returns_input_for_disabled_prefixer() -> None:
    prefixer = DynamodbPrefixer(hash_key="pk")

    assert prefixer.add_prefix(42) == 42


def test_compat_apply_handles_non_collection_when_enabled() -> None:
    prefixer = DynamodbPrefixer(hash_key="pk", hash_prefix="tenant#")

    assert prefixer.add_prefix("value") == "value"


def test_expression_values_skip_unmatched_placeholders() -> None:
    prefixer = DynamodbPrefixer(hash_key="pk", hash_prefix="tenant#")
    params = {
        "ExpressionAttributeValues": {
            ":pk": "1",
            ":other": 123,
        },
        "ExpressionAttributeNames": {"#id": "pk"},
    }

    updated = prefixer.apply_request(params, add=True)

    assert updated["ExpressionAttributeValues"][":pk"] == "tenant#1"
    assert updated["ExpressionAttributeValues"][":other"] == 123


def test_expression_values_ignore_untracked_aliases() -> None:
    prefixer = DynamodbPrefixer(hash_key="pk", hash_prefix="tenant#")
    params = {
        "ExpressionAttributeNames": {"#alt": "other"},
        "ExpressionAttributeValues": {":alt": "value"},
    }

    updated = prefixer.apply_request(params, add=True)

    assert updated["ExpressionAttributeValues"][":alt"] == "value"


def test_prefix_for_attribute_returns_range_prefix() -> None:
    prefixer = DynamodbPrefixer(
        hash_key="pk",
        hash_prefix="tenant#",
        range_key="sk",
        range_prefix="type#",
    )

    result = prefixer._DynamodbPrefixer__prefix_for_attribute("sk")  # type: ignore[attr-defined]

    assert result == "type#"


def test_prefix_for_attribute_returns_none_for_unknown_attribute() -> None:
    prefixer = DynamodbPrefixer(
        hash_key="pk",
        hash_prefix="tenant#",
        range_key="sk",
        range_prefix="type#",
    )

    result = prefixer._DynamodbPrefixer__prefix_for_attribute("other")  # type: ignore[attr-defined]

    assert result is None


def test_expression_values_support_non_string_payloads() -> None:
    prefixer = DynamodbPrefixer(
        hash_key="pk",
        hash_prefix="tenant#",
        range_key="sk",
        range_prefix="type#",
    )
    params = {
        "ExpressionAttributeNames": {"#rk": "sk"},
        "ExpressionAttributeValues": {":rk": 123},
    }

    updated = prefixer.apply_request(params, add=True)

    assert updated["ExpressionAttributeValues"][":rk"] == 123
