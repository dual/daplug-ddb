"""Integration tests for the DynamoDB adapter using the local stack."""

import unittest
import warnings

import boto3
from botocore.exceptions import ClientError

import daplug_ddb
from daplug_ddb import BatchItemException
from daplug_ddb.adapter import DynamodbAdapter

from tests.integration.mock_table import MockTable


class DynamoDBAdapterTest(unittest.TestCase):
    """Validates adapter CRUD operations against a local DynamoDB instance."""

    def setUp(self, *args, **kwargs):  # pylint: disable=unused-argument
        table_name = "unittestsort"
        warnings.simplefilter("ignore", ResourceWarning)
        self.maxDiff = None  # pylint: disable=invalid-name
        self.mock_table = MockTable(table_name=table_name)
        self.mock_table.setup_test_table()
        self.schema_args = {"schema": "test-dynamo-model"}
        self.adapter = daplug_ddb.adapter(
            table=table_name,
            endpoint="http://localhost:4000",
            schema_file="tests/openapi.yml",
            hash_key="test_id",
            idempotence_key="modified",
        )

    def tearDown(self):
        self.mock_table.clear_table()

    def test_init(self):
        self.assertIsInstance(self.adapter, DynamodbAdapter)

    def test_adapter_read(self):
        data = self.adapter.read(
            operation="get",
            query={
                "Key": {
                    "test_id": "abc123",
                    "test_query_id": "def345",
                }
            },
            **self.schema_args,
        )
        self.assertDictEqual(data, self.mock_table.mock_data)

    def test_adapter_read_scan(self):
        data = self.adapter.read(operation="scan", **self.schema_args)
        self.assertGreaterEqual(len(data), 1)

    def test_adapter_get(self):
        data = self.adapter.get(
            query={
                "Key": {
                    "test_id": "abc123",
                    "test_query_id": "def345",
                }
            },
            **self.schema_args,
        )
        self.assertDictEqual(data, self.mock_table.mock_data)

    def test_adapter_read_query(self):
        data = self.adapter.read(
            operation="query",
            query={
                "IndexName": "test_query_id",
                "Limit": 1,
                "KeyConditionExpression": "test_query_id = :test_query_id",
                "ExpressionAttributeValues": {":test_query_id": "def345"},
            },
            **self.schema_args,
        )
        self.assertDictEqual(data[0], self.mock_table.mock_data)

    def test_adapter_query(self):
        data = self.adapter.query(
            query={
                "IndexName": "test_query_id",
                "Limit": 1,
                "KeyConditionExpression": "test_query_id = :test_query_id",
                "ExpressionAttributeValues": {":test_query_id": "def345"},
            },
            **self.schema_args,
        )
        self.assertDictEqual(data[0], self.mock_table.mock_data)

    def test_adapter_raw_query(self):
        data = self.adapter.query(
            raw_query=True,
            query={
                "IndexName": "test_query_id",
                "Limit": 1,
                "KeyConditionExpression": "test_query_id = :test_query_id",
                "ExpressionAttributeValues": {":test_query_id": "def345"},
            },
            **self.schema_args,
        )
        passed = data["Items"][0] == self.mock_table.mock_data and data.get("LastEvaluatedKey")
        self.assertTrue(passed)

    def test_adapter_scan(self):
        data = self.adapter.scan(**self.schema_args)
        self.assertDictEqual(data[0], self.mock_table.mock_data)

    def test_adapter_raw_scan(self):
        data = self.adapter.scan(**{"raw_scan": True}, **self.schema_args)
        self.assertDictEqual(data["Items"][0], self.mock_table.mock_data)

    def test_adapter_create(self):
        new_data = {
            "test_id": "abc456",
            "test_query_id": "def789",
            "object_key": {"string_key": "nothing"},
            "array_number": [1, 2, 3],
            "array_objects": [{"array_string_key": "a", "array_number_key": 1}],
            "created": "2020-10-05",
            "modified": "2020-10-05",
        }
        data = self.adapter.create(data=new_data, **self.schema_args)
        self.assertDictEqual(data, new_data)

    def test_adapter_insert_same_hash_different_range(self):
        shared_hash = "duplicate-user"
        admin_item = {
            "test_id": shared_hash,
            "test_query_id": "admin",
            "object_key": {"string_key": "admin"},
            "array_number": [1, 2, 3],
            "array_objects": [{"array_string_key": "a", "array_number_key": 1}],
            "created": "2020-10-05",
            "modified": "2020-10-05",
        }
        basic_item = {
            "test_id": shared_hash,
            "test_query_id": "basic",
            "object_key": {"string_key": "basic"},
            "array_number": [3, 2, 1],
            "array_objects": [{"array_string_key": "b", "array_number_key": 2}],
            "created": "2020-10-06",
            "modified": "2020-10-06",
        }

        created_admin = self.adapter.create(data=admin_item, **self.schema_args)
        created_basic = self.adapter.create(data=basic_item, **self.schema_args)

        fetched_admin = self.adapter.get(
            query={"Key": {"test_id": shared_hash, "test_query_id": "admin"}},
            **self.schema_args,
        )
        fetched_basic = self.adapter.get(
            query={"Key": {"test_id": shared_hash, "test_query_id": "basic"}},
            **self.schema_args,
        )

        self.assertDictEqual(created_admin, fetched_admin)
        self.assertDictEqual(created_basic, fetched_basic)

    def test_adapter_insert_duplicate_composite_key_fails(self):
        shared_hash = "duplicate-user"
        composite_item = {
            "test_id": shared_hash,
            "test_query_id": "admin",
            "object_key": {"string_key": "admin"},
            "array_number": [9, 9, 9],
            "array_objects": [{"array_string_key": "a", "array_number_key": 9}],
            "created": "2020-10-05",
            "modified": "2020-10-05",
        }

        self.adapter.create(data=composite_item, **self.schema_args)
        with self.assertRaises(ClientError):
            self.adapter.create(data=composite_item, **self.schema_args)

    def test_adapter_batch_insert(self):
        item_list = {
            "data": [{"test_id": str(x), "test_query_id": str(x)} for x in range(100)],
            **self.schema_args,
        }
        self.adapter.batch_insert(**item_list)
        data = self.adapter.scan(**self.schema_args)
        self.assertEqual(len(data), 101)

    def test_adapter_batch_insert_fail(self):
        item_tuple = {"data": (1, 2, 3), **self.schema_args}
        self.assertRaises(BatchItemException, self.adapter.batch_insert, **item_tuple)

    def test_adapter_batch_delete(self):
        item_list = {
            "data": [{"test_id": str(x), "test_query_id": str(x)} for x in range(100)],
            **self.schema_args,
        }
        self.adapter.batch_insert(**item_list)
        data = self.adapter.scan(**self.schema_args)
        count_before_delete = len(data)
        self.adapter.batch_delete(**item_list)
        data = self.adapter.scan(**self.schema_args)
        self.assertTrue(count_before_delete == 101 and len(data) == 1)

    def test_adapter_batch_delete_fail(self):
        item_tuple = {"data": (1, 2, 3), **self.schema_args}
        self.assertRaises(BatchItemException, self.adapter.batch_delete, **item_tuple)

    def test_adapter_overwrite(self):
        new_data = {
            "test_id": "abc456",
            "test_query_id": "def789",
            "object_key": {"string_key": "nothing"},
            "array_number": [1, 2, 3],
            "array_objects": [{"array_string_key": "a", "array_number_key": 1}],
            "created": "2020-10-05",
            "modified": "2020-10-05",
        }
        data = self.adapter.create(operation="overwrite", data=new_data, **self.schema_args)
        self.assertDictEqual(data, new_data)

    def test_adapter_create_ignore_key(self):
        new_data = {
            "test_id": "abc456",
            "test_query_id": "def789",
            "ignore_key": True,
            "object_key": {"string_key": "nothing"},
            "array_number": [1, 2, 3],
            "array_objects": [{"array_string_key": "a", "array_number_key": 1}],
            "created": "2020-10-05",
            "modified": "2020-10-05",
        }
        data = self.adapter.create(data=new_data, **self.schema_args)
        new_data.pop("ignore_key", None)
        self.assertDictEqual(data, new_data)

    def test_adapter_update(self):
        new_data = {
            "test_id": "abc456-update",
            "test_query_id": "def789",
            "object_key": {"string_key": "nothing"},
            "array_number": [1, 2, 3],
            "array_objects": [{"array_string_key": "a", "array_number_key": 1}],
            "created": "2020-10-05",
            "modified": "2020-10-05",
        }
        data = self.adapter.create(data=new_data, **self.schema_args)
        self.assertDictEqual(data, new_data)
        new_data["array_number"] = [1, 2, 3, 4]
        updated_data = self.adapter.update(
            data=new_data,
            operation="get",
            query={
                "Key": {
                    "test_id": "abc456-update",
                    "test_query_id": "def789",
                }
            },
            **self.schema_args,
        )
        self.assertDictEqual(updated_data, new_data)

    def test_adapter_update_with_idempotence_key(self):
        updated_payload = {
            "test_id": "abc123",
            "test_query_id": "def345",
            "object_key": {"string_key": "updated"},
            "array_number": [1, 2, 3, 4],
            "array_objects": [{"array_string_key": "a", "array_number_key": 1}],
            "created": "2020-10-05",
            "modified": "2020-11-05",
        }

        result = self.adapter.update(
            data=updated_payload,
            operation="get",
            query={
                "Key": {
                    "test_id": "abc123",
                    "test_query_id": "def345",
                }
            },
            **self.schema_args,
        )

        self.assertDictEqual(result, updated_payload)
        stored = self.adapter.get(
            query={
                "Key": {
                    "test_id": "abc123",
                    "test_query_id": "def345",
                }
            },
            **self.schema_args,
        )
        self.assertEqual(stored["modified"], "2020-11-05")

    def test_adapter_update_idempotence_conflict(self):
        table = boto3.resource("dynamodb", endpoint_url="http://localhost:4000").Table(
            self.mock_table.table_name
        )

        conflicting_payload = {
            "test_id": "abc123",
            "test_query_id": "def345",
            "object_key": {"string_key": "stale"},
            "array_number": [1, 2, 3, 4],
            "array_objects": [{"array_string_key": "a", "array_number_key": 1}],
            "created": "2020-10-05",
            "modified": "2020-10-06",
        }

        original_put = self.adapter.table.put_item

        def conflicting_put(**kwargs):  # type: ignore[override]
            table.update_item(
                Key={"test_id": "abc123", "test_query_id": "def345"},
                UpdateExpression="SET modified = :m",
                ExpressionAttributeValues={":m": "2020-12-01"},
            )
            return original_put(**kwargs)

        self.adapter.table.put_item = conflicting_put  # type: ignore[assignment]
        try:
            with self.assertRaises(ClientError):
                self.adapter.update(
                    data=conflicting_payload,
                    operation="get",
                    query={
                        "Key": {
                            "test_id": "abc123",
                            "test_query_id": "def345",
                        }
                    },
                    **self.schema_args,
                )
        finally:
            self.adapter.table.put_item = original_put  # type: ignore[assignment]

    def test_adapter_update_use_latest_ignores_stale_data(self):
        adapter = daplug_ddb.adapter(
            table=self.mock_table.table_name,
            endpoint="http://localhost:4000",
            schema_file="tests/openapi.yml",
            hash_key="test_id",
            idempotence_key="modified",
            idempotence_use_latest=True,
        )
        schema_args = {"schema": "test-dynamo-model"}

        stale_payload = self.mock_table.mock_data.copy()
        stale_payload["modified"] = "2020-01-01"

        result = adapter.update(
            data=stale_payload,
            operation="get",
            query={"Key": {"test_id": "abc123", "test_query_id": "def345"}},
            **schema_args,
        )

        self.assertEqual(result["modified"], self.mock_table.mock_data["modified"])

    def test_adapter_update_use_latest_accepts_newer_data(self):
        adapter = daplug_ddb.adapter(
            table=self.mock_table.table_name,
            endpoint="http://localhost:4000",
            schema_file="tests/openapi.yml",
            hash_key="test_id",
            idempotence_key="modified",
            idempotence_use_latest=True,
        )
        schema_args = {"schema": "test-dynamo-model"}

        newer_payload = self.mock_table.mock_data.copy()
        newer_payload["modified"] = "2030-01-01"

        result = adapter.update(
            data=newer_payload,
            operation="get",
            query={"Key": {"test_id": "abc123", "test_query_id": "def345"}},
            **schema_args,
        )

        self.assertEqual(result["modified"], "2030-01-01")

    def test_adapter_update_use_latest_invalid_date(self):
        adapter = daplug_ddb.adapter(
            table=self.mock_table.table_name,
            endpoint="http://localhost:4000",
            schema_file="tests/openapi.yml",
            hash_key="test_id",
            idempotence_key="modified",
            idempotence_use_latest=True,
        )
        schema_args = {"schema": "test-dynamo-model"}

        invalid_payload = self.mock_table.mock_data.copy()
        invalid_payload["modified"] = "not-a-date"

        with self.assertRaises(ValueError):
            adapter.update(
                data=invalid_payload,
                operation="get",
                query={"Key": {"test_id": "abc123", "test_query_id": "def345"}},
                **schema_args,
            )

    def test_adapter_delete(self):
        new_data = {
            "test_id": "abc456-delete",
            "test_query_id": "def789",
            "object_key": {"string_key": "nothing"},
            "array_number": [1, 2, 3],
            "array_objects": [{"array_string_key": "a", "array_number_key": 1}],
            "created": "2020-10-05",
            "modified": "2020-10-05",
        }
        self.adapter.create(data=new_data, **self.schema_args)
        self.adapter.delete(
            query={
                "Key": {
                    "test_id": "abc456-delete",
                    "test_query_id": "def789",
                }
            },
            **self.schema_args,
        )
        deleted_data = self.adapter.get(
            query={
                "Key": {
                    "test_id": "abc456-delete",
                    "test_query_id": "def789",
                }
            },
            **self.schema_args,
        )
        self.assertDictEqual(deleted_data, {})

    def test_adapter_update_without_idempotence_key(self):
        adapter = daplug_ddb.adapter(
            table="unittestsort",  # table already exists from setUp
            endpoint="http://localhost:4000",
            schema_file="tests/openapi.yml",
            hash_key="test_id",
        )
        schema_args = {"schema": "test-dynamo-model"}

        new_data = {
            "test_id": "no-version",
            "test_query_id": "def789",
            "object_key": {"string_key": "nothing"},
            "array_number": [1, 2, 3],
            "array_objects": [{"array_string_key": "a", "array_number_key": 1}],
            "created": "2020-10-05",
            "modified": "2020-10-05",
        }
        created = adapter.create(data=new_data, **schema_args)
        self.assertDictEqual(created, new_data)

        new_data["array_number"] = [1, 2, 3, 4]
        updated = adapter.update(
            data=new_data,
            operation="get",
            query={
                "Key": {
                    "test_id": "no-version",
                    "test_query_id": "def789",
                }
            },
            **schema_args,
        )
        self.assertDictEqual(updated, new_data)


if __name__ == "__main__":
    unittest.main()
