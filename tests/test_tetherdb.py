import json
import os
import sys
import tempfile
import time
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

        self.db.backends.dynamodb_table = mock_table
        result = self.db.write_message("test_key", "test_value", backend="dynamodb")
        self.assertTrue(result)
        mock_table.put_item.assert_called_once_with(
            Item={"key": "test_key", "value": "test_value"}
        )

    @patch("etcd3gw.client.Etcd3Client.put")
    def test_write_message_etcd(self, mock_etcd_put):
        """Test writing a message to the etcd backend."""
        self.db.backends.etcd = Mock()
        result = self.db.write_message("test_key", "test_value", backend="etcd")
        self.assertTrue(result)
        self.db.backends.etcd.put.assert_called_once_with("test_key", "test_value")

    # --- Queued Write Tests ---
    def test_queued_write_processing(self):
        """Test that queued writes are processed correctly."""
        with patch("TetherDB.db.dbm.open", autospec=True) as mock_dbm_open:
            mock_db = {}
            mock_dbm_open.return_value.__enter__.return_value = mock_db

            # Disable the background worker's loop
            self.db.worker.is_running = False

            self.db.queue.put(("key1", "value1", "local"))
            self.db.queue.put(("key2", "value2", "local"))

            queued_batch = []
            while not self.db.queue.empty():
                queued_batch.append(self.db.queue.get())
            self.db._process_batch(queued_batch)

            self.assertEqual(mock_dbm_open.call_count, 2)
            self.assertEqual(mock_db["key1"], "value1")
            self.assertEqual(mock_db["key2"], "value2")

    # --- List Keys Tests ---
    @patch("dbm.open", autospec=True)
    def test_list_keys_local_pagination(self, mock_dbm_open):
        """Test listing keys from the local backend with pagination."""
        mock_db = Mock()
        mock_db.keys.return_value = [b"key1", b"key2", b"key3", b"key4", b"key5"]
        mock_dbm_open.return_value.__enter__.return_value = mock_db

        result = self.db.list_keys(page_size=2, backend="local")
        self.assertEqual(result["keys"], ["key1", "key2"])
        self.assertEqual(result["next_marker"], "key2")

    @patch("boto3.resource")
    def test_list_keys_dynamodb(self, mock_boto3):
        """Test listing keys from the DynamoDB backend."""
        mock_table = Mock()
        mock_boto3.return_value.Table.return_value = mock_table
        mock_table.meta.client.scan.return_value = {
            "Items": [{"key": "key1"}, {"key": "key2"}],
            "LastEvaluatedKey": {"key": "key2"},
        }

        self.db.backends.dynamodb_table = mock_table
        result = self.db.list_keys(page_size=2, backend="dynamodb")
        self.assertEqual(result["keys"], ["key1", "key2"])
        self.assertEqual(result["next_marker"], "key2")

    @patch("etcd3gw.client.Etcd3Client.get_prefix")
    def test_list_keys_etcd_pagination(self, mock_etcd_get):
        """Test listing keys from the etcd backend with pagination."""
        # Simulate etcd returning 3 keys
        mock_etcd_get.return_value.kvs = [
            Mock(key=b"key1"), Mock(key=b"key2"), Mock(key=b"key3")
        ]

        self.db.backends.etcd = Mock()
        self.db.backends.etcd.get_prefix = mock_etcd_get

        result = self.db.list_keys(page_size=2, backend="etcd")
        self.assertEqual(result["keys"], ["key1", "key2"])
        self.assertEqual(result["next_marker"], "key2")

    # --- Tether Decorator Tests ---
    @patch.object(DB, "write_message", return_value=True)
    def test_tether_decorator(self, mock_write_message):
        """Test the tether decorator for direct writes."""
        @self.db.tether(bucket="test_bucket", backend="local", queue=True)
        def example_func():
            return {"key": "decorator_key", "value": {"nested": "decorator_value"}}

        result = example_func()
        self.assertTrue(result)
        mock_write_message.assert_called_once_with(
            "decorator_key",
            {"nested": "decorator_value"},
            "test_bucket",
            "local",
            queue=True,
        )

    @patch.object(DB, "write_message", return_value=True)
    def test_tether_decorator_invalid_output(self, mock_write_message):
        """Test that tether decorator raises an error for invalid function return."""
        @self.db.tether(bucket="test_bucket", backend="local", queue=True)
        def invalid_func():
            return {"invalid_key": "no_value"}

        with self.assertRaises(ValueError):
            invalid_func()

    # --- Batch Timeout Tests ---
    def test_worker_batch_interval(self):
        """Test that the worker respects the batch timeout."""
        self.db.worker.batch_interval = 0.1
        self.db.queue.put(("key1", "value1", "local"))
        self.db.queue.put(("key2", "value2", "local"))
        time.sleep(0.3)
        self.assertTrue(self.db.queue.empty(), "Queue not processed within timeout")


if __name__ == "__main__":
    unittest.main()