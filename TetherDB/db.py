from functools import wraps
import json
import threading
from queue import Queue
import uuid

from .base_logger import initialize_logger
from .backends import BackendInitializer
from .background_worker import BackgroundWorker
from .key_utils import build_key


class DB:
    """
    TetherDB: A hybrid key-value database supporting multiple backends (local, DynamoDB, and etcd).
    Provides methods to:
    - Write messages (immediate or queued)
    - Read messages
    - Update existing messages
    - List messages with pagination
    - Decorate functions to tether their outputs to the database.
    """

    def __init__(self, config: dict = None, config_file: str = None):
        """
        Initialize the TetherDB instance.

        :param config: A dictionary containing configuration settings (optional).
        :param config_file: Path to a JSON configuration file (optional).
        :raises ValueError: If neither or both config and config_file are provided.
        """
        if config and config_file:
            raise ValueError("Provide either 'config' or 'config_file', not both.")
        self.config = config or self._load_config_from_file(config_file)
        self.logger = initialize_logger(self.config)
        self._db_lock = threading.Lock()
        self.backends = BackendInitializer(self.config, self.logger)

        # Initialize background worker for queued writes
        self.queue = Queue()
        self.worker = BackgroundWorker(self.queue, self.backends, self.logger)
        self._worker_started = False

    def _load_config_from_file(self, config_file):
        """Load configuration from a JSON file."""
        with open(config_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def start(self):
        """Start the background worker for processing queued writes."""
        if not self._worker_started:
            self.worker.start(
                self.config.get("queue_batch", {}).get("size", 15),
                self.config.get("queue_batch", {}).get("interval", 1),
            )
            self._worker_started = True
            self.logger.debug("Background worker started.")

    def stop(self):
        """Stop the background worker gracefully, ensuring pending writes are processed."""
        if self._worker_started:
            self.worker.stop()
            self._worker_started = False
            self.logger.debug("Background worker stopped.")

    def __enter__(self):
        """Start the worker when entering the context."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure the worker stops when exiting the context."""
        self.stop()

    def write_message(self, key, value, bucket="", backend="local", queue=False):
        """
        Write a key-value pair to the specified backend.

        :param key: Key to store the value.
        :param value: Value to store; must be JSON-serializable.
        :param bucket: Optional bucket prefix for organizing keys.
        :param backend: Target backend to write to (`local`, `dynamodb`, or `etcd`).
        :param queue: If True, the write is queued for background processing.
        """
        full_key = build_key(bucket, key)
        value = json.dumps(value) if isinstance(value, dict) else value

        if queue:
            if not self.worker.is_running:
                raise RuntimeError(
                    "Background worker is not running. "
                    "Start the worker before queuing messages using 'start()'."
                )
            self.queue.put((full_key, value, backend))
            self.logger.debug(f"Message queued: {full_key}")
            return True

        # Acquire the lock before writing directly
        with self._db_lock:
            return self.backends.write(full_key, value, backend)

    def read_message(self, key, bucket="", backend="local"):
        """
        Retrieve a value associated with a key from the specified backend.

        :param key: Key to retrieve the value.
        :param bucket: Optional bucket prefix.
        :param backend: Target backend (`local`, `dynamodb`, or `etcd`).
        :return: Value associated with the key, or None if not found.
        """
        full_key = build_key(bucket, key)

        # Acquire the lock before reading
        with self._db_lock:
            return self.backends.read(full_key, backend)

    def update_message(self, key, value, bucket="", backend="local"):
        """
        Update an existing key-value pair in the specified backend.

        :param key: Key to update.
        :param value: New value to update; must be JSON-serializable.
        :param bucket: Optional bucket prefix.
        :param backend: Target backend (`local`, `dynamodb`, or `etcd`).
        :return: True if updated successfully, False otherwise.
        """
        full_key = build_key(bucket, key)
        value = json.dumps(value) if isinstance(value, dict) else value

        # Acquire the lock before updating
        with self._db_lock:
            return self.backends.update(full_key, value, backend)

    def list_messages(self, page_size=10, start_after=None, bucket="", backend="local"):
        """
        List messages (values) from the specified backend with pagination support.

        :param page_size: Number of messages to return.
        :param start_after: Start key for pagination.
        :param bucket: Optional bucket prefix.
        :param backend: Target backend.
        :return: A dictionary containing:
                - 'messages': List of values (as raw JSON strings).
                - 'next_marker': Marker for the next page.
        """
        keys_result = self.backends.list_messages(
            page_size=page_size, start_after=start_after, bucket=bucket, backend=backend
        )
        messages = keys_result["messages"]
        messages = [
            json.dumps(msg) if isinstance(msg, dict) else msg for msg in messages
        ]

        return {"messages": messages, "next_marker": keys_result["next_marker"]}

    def tether(self, bucket="", queue=False, backend="local"):
        """
        Decorator to write the return value of a function to the database.

        The function must return a dictionary containing:
        - "key": Optional custom key (a UUID will be generated if not provided).
        - "value": The data to store.

        :param bucket: Optional bucket prefix.
        :param queue: If True, the write is queued for background processing.
        :param backend: Target backend (`local`, `dynamodb`, or `etcd`).
        :return: Decorated function.
        """

        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                result = func(*args, **kwargs)
                if isinstance(result, dict) and "value" in result:
                    key = result.get("key", str(uuid.uuid4()))
                    value = result["value"]
                    self.write_message(key, value, bucket, backend, queue)
                    return True
                raise ValueError(
                    "Function return value must be a dictionary containing 'value'."
                )

            return wrapper

        return decorator
