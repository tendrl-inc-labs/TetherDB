import json
import os
import sys
import tempfile
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
            "queue_batch": {"size": 2, "timeout": 1.0},
            "local": {"filepath": "test_local.db"},
            "dynamodb": {"table_name": "TestTable"},
            "etcd": {"host": "localhost", "port": 2379},
        }

    def setUp(self):
        """Set up a fresh DB instance for each test."""
        self.db = DB(config=self.config)

    def tearDown(self):
        """Clean up resources after each test."""
        self.db.stop()  # Gracefully stop the worker
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
    @patch("boto3.resource")
    def test_write_message_dynamodb(self, mock_boto3):
        """Test writing a message to the DynamoDB backend."""
        mock_table = Mock()
        mock_boto3.return_value.Table.return_value = mock_table

        self.db.backends.dynamodb_table = mock_table  # Mock the backend table
        result = self.db.write_message("test_key", "test_value", backend="dynamodb")
        self.assertTrue(result)
        mock_table.put_item.assert_called_once_with(
            Item={"key": "test_key", "value": "test_value"}
        )

    @patch("etcd3gw.client.Etcd3Client.put")
    def test_write_message_etcd(self, mock_etcd_put):
        """Test writing a message to the etcd backend."""
        self.db.backends.etcd = Mock()  # Mock etcd client
        result = self.db.write_message("test_key", "test_value", backend="etcd")
        self.assertTrue(result)
        self.db.backends.etcd.put.assert_called_once_with("test_key", "test_value")

        # --- Queued Write Tests ---
        @patch("TetherDB.db.dbm.open", autospec=True)
        def test_queued_write_processing(self, mock_dbm_open):
            """Test that queued writes are processed correctly."""
            self.db.backends.local_db_file = self.config["local"][
                "filepath"
            ]  # Ensure local backend file is set

            # Queue two messages for processing
            self.db.write_message("key1", "value1", backend="local", queue=True)
            self.db.write_message("key2", "value2", backend="local", queue=True)

            # Allow time for the background worker to process the queue
            import time

            time.sleep(3)

            self.db.stop()  # Stop the worker gracefully

            # Verify dbm.open was called twice
            self.assertEqual(mock_dbm_open.call_count, 2)

            # Verify the correct file path and mode were passed to dbm.open
            mock_dbm_open.assert_any_call(self.config["local"]["filepath"], "c")

    # --- Tether Decorator Tests ---
    @patch.object(DB, "write_message", return_value=True)
    def test_tether_decorator(self, mock_write_message):
        """Test the tether decorator for direct writes."""

        @self.db.tether(bucket="test_bucket", backend="local", wait=True)
        def example_func():
            return {"key": "decorator_key", "value": {"nested": "decorator_value"}}

        result = example_func()
        self.assertTrue(result)
        mock_write_message.assert_called_once_with(
            "decorator_key",
            '{"nested": "decorator_value"}',
            "test_bucket",
            "local",
            False,
        )

    @patch.object(DB, "write_message", return_value=True)
    def test_tether_decorator_invalid_output(self, mock_write_message):
        """Test that tether decorator raises an error for invalid function return."""

        @self.db.tether(bucket="test_bucket", backend="local", wait=True)
        def invalid_func():
            return {"invalid_key": "no_value"}

        with self.assertRaises(ValueError):
            invalid_func()


if __name__ == "__main__":
    unittest.main()
