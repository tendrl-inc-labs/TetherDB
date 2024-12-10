import dbm
from functools import wraps
import json
from queue import Queue
import threading
import uuid

from .base_logger import initialize_logger
from .backends import BackendInitializer
from .background_worker import BackgroundWorker
from .key_utils import build_key, paginate_keys


class DB:
    """
    Main database class handling writes, listing, tethered functions, and batching.
    """

    __slots__ = ["config", "logger", "backends", "queue", "_db_lock", "worker"]

    def __init__(self, config: dict = None, config_file: str = None):
        """
        Initialize the DB class.

        :param config: A dictionary with configuration settings (optional).
        :param config_file: Path to the configuration JSON file (optional).
        :raises ValueError: If neither or both config and config_file are provided.
        """
        if config and config_file:
            raise ValueError("Provide either 'config' or 'config_file', not both.")

        if config:
            self.config = config
        elif config_file:
            with open(config_file, "r", encoding="utf-8") as f:
                self.config = json.load(f)
        else:
            raise ValueError("Either 'config' or 'config_file' must be provided.")

        # Initialize logger, backends, queue, and worker
        self.logger = initialize_logger(self.config)
        self.backends = BackendInitializer(self.config, self.logger)
        self.queue = Queue()
        self._db_lock = threading.Lock()
        self.worker = BackgroundWorker(
            self.queue, self._process_batch, self.logger, self._db_lock
        )

        # Automatically start the worker
        self.start()

    def start(self):
        """Start the background worker for queued writes."""
        self.worker.start(
            self.config.get("queue_batch", {}).get("size", 10),
            self.config.get("queue_batch", {}).get("timeout", 2.0),
        )

    def stop(self):
        """Stop the background worker gracefully."""
        self.worker.stop()

    def write_message(
        self,
        key: str,
        value: str,
        bucket: str = "",
        backend: str = "local",
        queue: bool = False,
    ):
        """
        Write a key-value pair directly to a backend or queue it for batch processing.

        :param key: Key for the data.
        :param value: The data to store. Must be JSON serializable.
        :param bucket: Optional bucket prefix.
        :param backend: Backend to write to ('local', 'dynamodb', or 'etcd').
        :param queue: If True, queue the write for background processing.
        :raises ValueError: If the value is not JSON serializable or the backend is invalid.
        """
        self._validate_backend(backend)
        full_key = build_key(bucket, key)
        try:
            if isinstance(value, dict):
                value = json.dumps(value)
            else:
                json.dumps(value)

            if queue:
                self.queue.put((full_key, value, backend))
                self.logger.debug(f"Message queued: {full_key}")
                return True

            with self._db_lock:
                self._write_to_backend(full_key, value, backend)
            return True
        except (ValueError, TypeError) as e:
            self.logger.error(f"Error writing message: {e}")
            raise ValueError("Invalid value: Must be JSON serializable")

    def tether(self, bucket: str = "", queue: bool = False, backend: str = "local"):
        """
        A decorator to write the return value of a function to the database.

        The function must return a dictionary with:
        - "key": Optional custom key (str). A UUID is generated if not provided.
        - "value": The data to store (str or dict).

        :param bucket: Optional bucket prefix.
        :param queue: If True, writes immediately; if False, queues the write.
        :param backend: Backend to write to: 'local', 'dynamodb', or 'etcd'.
        """

        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                result = func(*args, **kwargs)
                if isinstance(result, dict) and "value" in result:
                    key = result.get("key", str(uuid.uuid4()))
                    value = result["value"]
                    self.write_message(key, value, bucket, backend, queue=queue)
                    return True
                else:
                    raise ValueError(
                        "Function return value must be a dictionary containing a 'value' key."
                    )

            return wrapper

        return decorator

    def _validate_backend(self, backend):
        """
        Validates that the backend is configured and ready to use.

        :param backend: The backend to validate ('local', 'dynamodb', 'etcd').
        :raises ValueError: If the backend is not configured.
        """
        if backend == "local" and not self.backends.local_db_file:
            raise ValueError("Local backend is not configured in the config file.")
        elif backend == "dynamodb" and not self.backends.dynamodb_table:
            raise ValueError("DynamoDB backend is not configured in the config file.")
        elif backend == "etcd" and not self.backends.etcd:
            raise ValueError("etcd backend is not configured in the config file.")
        elif backend not in {"local", "dynamodb", "etcd"}:
            raise ValueError(f"Unsupported backend: {backend}")

    def _process_batch(self, batch):
        """
        Process a batch of messages using the shared lock.

        :param batch: List of messages to process.
        """
        try:
            with self._db_lock:
                for full_key, value, backend in batch:
                    self._write_to_backend(full_key, value, backend)
        except Exception as e:
            self.logger.error(f"Error processing batch: {e}")

    def _write_to_backend(self, key, value, backend):
        """
        Write a single key-value pair to the specified backend.

        :param key: Full key for the data.
        :param value: Data to write.
        :param backend: Backend to write to.
        """
        if backend == "local" and self.backends.local_db_file:
            with dbm.open(self.backends.local_db_file, "c") as db:
                db[key] = value
                self.logger.debug(f"Written to Local: {key}")
        elif backend == "dynamodb" and self.backends.dynamodb_table:
            self.backends.dynamodb_table.put_item(Item={"key": key, "value": value})
            self.logger.debug(f"Written to DynamoDB: {key}")
        elif backend == "etcd" and self.backends.etcd:
            self.backends.etcd.put(key, value)
            self.logger.debug(f"Written to etcd: {key}")
        else:
            raise ValueError(f"Invalid backend or backend not initialized: {backend}")

    def list_keys(
        self,
        page_size: int = 10,
        start_after: str = None,
        bucket: str = "",
        backend: str = "local",
    ):
        """
        List all keys from the specified backend with optional pagination and bucket filtering.

        :param page_size: Number of keys to list at once.
        :param start_after: Start listing keys after this key (pagination marker).
        :param bucket: Optional bucket prefix for filtering keys.
        :param backend: Backend to list keys from ('local', 'dynamodb', 'etcd').
        :return: A tuple (list of keys, next_marker).
        """
        self._validate_backend(backend)

        if backend == "local":
            return self._list_local_keys(page_size, start_after, bucket)
        elif backend == "dynamodb":
            return self._list_dynamodb_keys(page_size, start_after, bucket)
        elif backend == "etcd":
            return self._list_etcd_keys(page_size, start_after, bucket)
        else:
            raise ValueError(f"Unsupported backend: {backend}")

    def _list_local_keys(self, page_size, start_after, bucket):
        """List keys from the local backend."""
        with dbm.open(self.backends.local_db_file, "r") as index_db:
            all_keys = sorted(k.decode("utf-8") for k in index_db.keys())
            if bucket:
                all_keys = [k for k in all_keys if k.startswith(build_key(bucket, ""))]
        return paginate_keys(all_keys, page_size, start_after)

    def _list_dynamodb_keys(self, page_size, start_after, bucket):
        """List keys from DynamoDB."""
        scan_args = {
            "TableName": self.backends.dynamodb_table.name,
            "ProjectionExpression": "#k",
            "ExpressionAttributeNames": {"#k": "key"},
            "Limit": page_size,
        }

        if start_after:
            scan_args["ExclusiveStartKey"] = {"key": start_after}

        response = self.backends.dynamodb_table.meta.client.scan(**scan_args)

        keys = [item["key"] for item in response.get("Items", [])]

        if bucket:
            keys = [k for k in keys if k.startswith(build_key(bucket, ""))]

        next_marker = response.get("LastEvaluatedKey", {}).get("key")

        return {"keys": keys, "next_marker": next_marker}

    def _list_etcd_keys(self, page_size, start_after, bucket):
        """
        List keys from etcd backend.

        :param page_size: Number of keys to fetch.
        :param start_after: Key to start after (pagination).
        :param bucket: Bucket prefix for filtering keys.
        :return: A tuple (keys, next_marker).
        """
        prefix = bucket if bucket else ""

        try:
            response = self.backends.etcd.get_prefix(prefix, limit=page_size)
            keys = [kv.key.decode("utf-8") for kv in response.kvs]

            return paginate_keys(keys, page_size, start_after)

        except Exception as e:
            self.logger.error(f"Error listing etcd keys: {e}")
            return [], None
