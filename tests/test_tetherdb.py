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

    def tearDown(self):
        """Clean up resources after each test."""
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
        db = DB(config=self.config)
        db.start()
        db.write_message("key1", "value1", backend="local")
        value = db.read_message("key1", backend="local")
        self.assertEqual(value, "value1")
        db.stop()

    def test_read_message_local_with_context_manager(self):
        """Test reading a message using the context manager."""
        with DB(config=self.config) as db:
            db.start()
            db.write_message("key1", "value1", backend="local")
            value = db.read_message("key1", backend="local")
            self.assertEqual(value, "value1")

    @patch("boto3.resource")
    def test_read_message_dynamodb(self, mock_boto3):
        """Test reading a message from the DynamoDB backend."""
        mock_table = Mock()
        mock_table.get_item.return_value = {"Item": {"value": "value1"}}
        mock_boto3.return_value.Table.return_value = mock_table

        db = DB(config=self.config)
        db.backends.dynamodb_table = mock_table
        value = db.read_message("key1", backend="dynamodb")
        self.assertEqual(value, "value1")

    @patch("etcd3gw.client.Etcd3Client.get")
    def test_read_message_etcd(self, mock_etcd_get):
        """Test reading a message from the etcd backend."""

        # Simulate proper etcd.get() return value
        mock_etcd_get.return_value = iter(
            [b'{"data": "value1"}']
        )  # Iterator returning bytes

        with DB(config=self.config) as db:
            db.start()
            value = db.read_message("key1", backend="etcd")
            self.assertEqual(
                value, {"data": "value1"}, "The returned value does not match."
            )

        mock_etcd_get.assert_called_once_with("key1")

    # --- Update Message Tests ---
    def test_update_message_local(self):
        """Test updating a message in the local backend."""
        db = DB(config=self.config)
        db.start()
        db.write_message("key1", "initial", backend="local")
        self.assertTrue(db.update_message("key1", "updated", backend="local"))
        updated_value = db.read_message("key1", backend="local")
        self.assertEqual(updated_value, "updated")
        db.stop()

    @patch("etcd3gw.client.Etcd3Client.get")
    @patch("etcd3gw.client.Etcd3Client.put")
    def test_update_message_etcd(self, mock_etcd_put, mock_etcd_get):
        """Test updating a message in the etcd backend."""

        # Mock etcd.get to return an existing key
        mock_etcd_get.return_value = iter([b'{"data": "initial_value"}'])
        mock_etcd_put.return_value = True  # Simulate successful put

        with DB(config=self.config) as db:
            db.start()
            result = db.update_message(
                "key1", {"data": "updated_value"}, backend="etcd"
            )
            self.assertTrue(
                result, "Update should return True for a successful update."
            )

        mock_etcd_get.assert_called_once_with("key1")
        mock_etcd_put.assert_called_once_with("key1", '{"data": "updated_value"}')

    # --- List Messages Tests ---
    def test_list_messages_local(self):
        """Test listing messages (values) from the local backend."""
        db = DB(config=self.config)
        db.start()
        db.write_message("key1", '{"data": "value1"}', backend="local")
        db.write_message("key2", '{"data": "value2"}', backend="local")
        result = db.list_messages(page_size=2, backend="local")
        self.assertEqual(len(result["messages"]), 2)
        self.assertIn('{"data": "value1"}', result["messages"])
        db.stop()

    # --- Queue Write Tests ---
    def test_queue_write_without_worker(self):
        """Test queuing a write message when the worker is not started."""
        db = DB(config=self.config)
        with self.assertRaises(RuntimeError):
            db.write_message("key1", "value1", backend="local", queue=True)

    # --- Tether Decorator Tests ---
    def test_tether_decorator(self):
        """Test the tether decorator for writing function return values."""
        db = DB(config=self.config)
        db.start()

        @db.tether(bucket="test_bucket", backend="local")
        def example_function():
            return {"key": "tether_key", "value": "tether_value"}

        result = example_function()
        self.assertTrue(result)
        value = db.read_message("tether_key", "test_bucket", "local")
        self.assertEqual(value, "tether_value")
        db.stop()
    
    def test_tether_decorator_invalid_value(self):
        """
        Test the tether decorator raises an error when the 'value' is not a dict or string.
        """
        db = DB(config=self.config)

        # Function that returns an invalid 'value' type (e.g., an integer)
        @db.tether(bucket="test_bucket", backend="local")
        def invalid_function():
            return {"key": "invalid_key", "value": 123}  # Invalid 'value' type

        with self.assertRaises(ValueError) as context:
            invalid_function()
        
        self.assertIn(
            "The 'value' field must be a dict or string",
            str(context.exception),
            "Expected ValueError for invalid 'value' type."
        )


if __name__ == "__main__":
    unittest.main()
