import json
import os
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.append(os.path.abspath(".."))

from TetherDB.db import DB


class TestTetherDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up common variables for tests."""
        cls.config = {
            "logging": "none",
            "queue_batch": {"size": 2, "timeout": 1.0},
            "local": {"filepath": "test_local.db"},
            "dynamodb": {"table_name": "TestTable"},
            "etcd": {"host": "localhost", "port": 2379}
        }

    def setUp(self):
        """Set up a fresh DB instance for each test."""
        self.db = DB(config=self.config)

    def tearDown(self):
        """Clean up resources after each test."""
        if os.path.exists(self.config["local"]["filepath"]):
            os.remove(self.config["local"]["filepath"])

    # --- Initialization Tests ---
    def test_init_with_config(self):
        """Test initialization with a config dictionary."""
        db = DB(config=self.config)
        self.assertIsNotNone(db)

    def test_init_with_config_file(self):
        """Test initialization with a config file."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            json.dump(self.config, temp_file)
            temp_file_path = temp_file.name

        db = DB(config_file=temp_file_path)
        self.assertIsNotNone(db)
        os.remove(temp_file_path)

    def test_init_with_both_config_and_file(self):
        """Test that passing both config and config_file raises an error."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            json.dump(self.config, temp_file)
            temp_file_path = temp_file.name

        with self.assertRaises(ValueError):
            DB(config=self.config, config_file=temp_file_path)
        os.remove(temp_file_path)

    # --- Backend Validation Tests ---
    def test_invalid_backend(self):
        """Test writing to an unconfigured backend."""
        with self.assertRaises(ValueError):
            self.db.write_message("key", "value", backend="invalid_backend")

    # --- Direct Write Tests ---
    def test_write_message_local(self):
        """Test writing a message to the local backend."""
        result = self.db.write_message("test_key", "test_value", backend="local")
        self.assertTrue(result)

    @patch("TetherDB.backends.boto3.resource")
    def test_write_message_dynamodb(self, mock_boto3):
        """Test writing a message to the DynamoDB backend."""
        mock_table = Mock()
        mock_boto3.return_value.Table.return_value = mock_table

        result = self.db.write_message("test_key", "test_value", backend="dynamodb")
        self.assertTrue(result)
        mock_table.put_item.assert_called_once()

    @patch("TetherDB.backends.etcd3gw.client.Etcd3Client.put")
    def test_write_message_etcd(self, mock_etcd_put):
        """Test writing a message to the etcd backend."""
        result = self.db.write_message("test_key", "test_value", backend="etcd")
        self.assertTrue(result)
        mock_etcd_put.assert_called_once_with("test_key", "test_value")

    # --- Queued Write Tests ---
    @patch("TetherDB.backends.dbm.open")
    def test_queued_write_processing(self, mock_dbm_open):
        """Test that queued writes are processed correctly."""
        self.db.start()
        self.db.write_message("key1", "value1", backend="local", queue=True)
        self.db.write_message("key2", "value2", backend="local", queue=True)
        self.db.stop()

        # Verify that dbm.open was called to process the writes
        self.assertTrue(mock_dbm_open.called)

    # --- Tether Decorator Tests ---
    def test_tether_decorator(self):
        """Test the tether decorator for direct writes."""
        @self.db.tether(bucket="test_bucket", backend="local", wait=True)
        def example_func():
            return {"key": "decorator_key", "value": "decorator_value"}

        result = example_func()
        self.assertTrue(result)

    def test_tether_decorator_invalid_output(self):
        """Test that tether decorator raises an error for invalid function return."""
        @self.db.tether(bucket="test_bucket", backend="local", wait=True)
        def invalid_func():
            return {"invalid_key": "no_value"}

        with self.assertRaises(ValueError):
            invalid_func()

    # --- Edge Case Tests ---
    def test_write_message_invalid_value(self):
        """Test writing a message with an invalid value type."""
        with self.assertRaises(ValueError):
            self.db.write_message("key", 12345, backend="local")  # Non-JSON serializable


if __name__ == "__main__":
    unittest.main()