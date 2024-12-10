import os
import sys
import unittest
from unittest.mock import Mock, patch

# Add the project root to sys.path
sys.path.append(os.path.abspath(".."))

from TetherDB.db import DB


class TestTetherDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up common variables for tests."""
        cls.config = {
            "logging": "none",
            "queue_batch": {"size": 2, "interval": 1.0},
            "local": {"filepath": "test_local.db"},
            "dynamodb": {"table_name": "TestTable"},
            "etcd": {"host": "localhost", "port": 2379},
        }

    def setUp(self):
        """Set up a fresh DB instance for each test."""
        self.db = DB(config=self.config)

    def tearDown(self):
        """Clean up resources after each test."""
        self.db.stop()
        if os.path.exists(self.config["local"]["filepath"]):
            os.remove(self.config["local"]["filepath"])

    # --- Initialization Tests ---
    def test_init_with_config(self):
        """Test initialization with a config dictionary."""
        db = DB(config=self.config)
        self.assertIsNotNone(db)

    # --- Read Message Tests ---
    def test_read_message_local(self):
        """Test reading a message from the local backend."""
        self.db.write_message("key1", "value1", backend="local")
        value = self.db.read_message("key1", backend="local")
        self.assertEqual(value, "value1")

    @patch("boto3.resource")
    def test_read_message_dynamodb(self, mock_boto3):
        """Test reading a message from the DynamoDB backend."""
        mock_table = Mock()
        mock_table.get_item.return_value = {"Item": {"value": "value1"}}
        mock_boto3.return_value.Table.return_value = mock_table

        self.db.backends.dynamodb_table = mock_table
        value = self.db.read_message("key1", backend="dynamodb")
        self.assertEqual(value, "value1")

    @patch("etcd3gw.client.Etcd3Client.get")
    def test_read_message_etcd(self, mock_etcd_get):
        """Test reading a message from the etcd backend."""
        mock_etcd_get.return_value = Mock(kvs=[Mock(value=b"value1")])

        self.db.backends.etcd = Mock()
        self.db.backends.etcd.get = mock_etcd_get

        value = self.db.read_message("key1", backend="etcd")
        self.assertEqual(value, "value1")

    # --- Update Message Tests ---
    def test_update_message_local(self):
        """Test updating a message in the local backend."""
        self.db.write_message("key1", "initial", backend="local")
        self.assertTrue(self.db.update_message("key1", "updated", backend="local"))

        updated_value = self.db.read_message("key1", backend="local")
        self.assertEqual(updated_value, "updated")

    @patch("boto3.resource")
    def test_update_message_dynamodb(self, mock_boto3):
        """Test updating a message in the DynamoDB backend."""
        mock_table = Mock()
        mock_table.update_item.return_value = {"Attributes": {"value": "updated"}}
        mock_boto3.return_value.Table.return_value = mock_table

        self.db.backends.dynamodb_table = mock_table
        result = self.db.update_message("key1", "updated", backend="dynamodb")
        self.assertTrue(result)

    @patch("etcd3gw.client.Etcd3Client.put")
    @patch("etcd3gw.client.Etcd3Client.get")
    def test_update_message_etcd(self, mock_etcd_get, mock_etcd_put):
        """Test updating a message in the etcd backend."""
        mock_etcd_get.return_value = Mock(kvs=[Mock(value=b"initial")])
        self.db.backends.etcd = Mock()
        self.db.backends.etcd.get = mock_etcd_get
        self.db.backends.etcd.put = mock_etcd_put

        result = self.db.update_message("key1", "updated", backend="etcd")
        self.assertTrue(result)
        mock_etcd_put.assert_called_once_with("key1", "updated")

    # --- List Messages Tests ---
    def test_list_messages_local(self):
        """Test listing messages (values) from the local backend."""
        # Populate local backend
        self.db.write_message("key1", '{"data": "value1"}', backend="local")
        self.db.write_message("key2", '{"data": "value2"}', backend="local")

        # Call list_messages
        result = self.db.list_messages(page_size=2, backend="local")
        self.assertEqual(len(result["messages"]), 2)
        self.assertIn('{"data": "value1"}', result["messages"])
        self.assertIn('{"data": "value2"}', result["messages"])

    @patch("boto3.resource")
    def test_list_messages_dynamodb(self, mock_boto3):
        """Test listing messages (values) from the DynamoDB backend."""
        # Mock the DynamoDB table resource
        mock_table = Mock()
        mock_table.meta.client.scan.return_value = {
            "Items": [
                {"key": "key1", "value": '{"data": "value1"}'},
                {"key": "key2", "value": "value2"},
            ],
            "LastEvaluatedKey": None,
        }

        mock_boto3.return_value.Table.return_value = mock_table

        # Reinitialize the DB backend with the mock
        self.db.backends.dynamodb_table = mock_table

        # Call the method under test
        result = self.db.list_messages(page_size=2, backend="dynamodb")

        # Debug output
        print("Final Test Output:", result)

        # Assertions (compare with string representation)
        self.assertEqual(
            len(result["messages"]), 2, "Expected two messages in DynamoDB result"
        )
        self.assertIn('{"data": "value1"}', result["messages"])
        self.assertIn("value2", result["messages"])

    @patch("etcd3gw.client.Etcd3Client.get_prefix")
    def test_list_messages_etcd(self, mock_etcd_get):
        """Test listing messages (values) from the etcd backend."""
        # Mock etcd response
        mock_etcd_get.return_value.kvs = [
            Mock(value=b'{"data": "value1"}', key=b"key1"),
            Mock(value=b'{"data": "value2"}', key=b"key2"),
        ]

        # Call list_messages
        result = self.db.list_messages(page_size=2, backend="etcd")
        self.assertEqual(len(result["messages"]), 2)
        self.assertIn('{"data": "value1"}', result["messages"])
        self.assertIn('{"data": "value2"}', result["messages"])

    # --- Tether Decorator ---
    def test_tether_decorator(self):
        """Test the tether decorator for writing function return values."""

        @self.db.tether(bucket="test_bucket", backend="local")
        def example_function():
            return {"key": "tether_key", "value": "tether_value"}

        result = example_function()
        self.assertTrue(result)
        value = self.db.read_message("tether_key", "test_bucket", "local")
        self.assertEqual(value, "tether_value")


if __name__ == "__main__":
    unittest.main()
