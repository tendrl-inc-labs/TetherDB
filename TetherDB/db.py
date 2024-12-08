import dbm
import json
from queue import Queue
import threading

from .base_logger import initialize_logger
from .backends import BackendInitializer
from .background_worker import BackgroundWorker
from .key_utils import build_key
from .decorators import tether


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

        self.logger = initialize_logger(self.config)
        self.backends = BackendInitializer(self.config, self.logger)
        self.queue = Queue()
        self._db_lock = threading.Lock()
        self.worker = BackgroundWorker(
            self.queue, self._process_batch, self.logger, self._db_lock
        )

        self.worker.start(
            self.config.get("queue_batch", {}).get("size", 10),
            self.config.get("queue_batch", {}).get("timeout", 2.0),
        )

    def write_message(
        self, key: str, value: str, bucket="", backend="local", queue=False
    ):
        """
        Write a key-value pair directly or queue it for batch processing.

        :param key: Key for the data.
        :param value: The data to store.
        :param bucket: Optional bucket prefix.
        :param backend: Backend to write to.
        :param queue: If True, queue the write for batch processing.
        """
        self._validate_backend(backend)
        full_key = build_key(bucket, key)
        try:
            if isinstance(value, dict):
                value = json.dumps(value)

            if queue:
                self.queue.put((full_key, value, backend))
                self.logger.debug(f"Message queued: {full_key}")
                return True

            # Use the shared lock for direct writes
            with self._db_lock:
                self._write_to_backend(full_key, value, backend)
            return True
        except Exception as e:
            self.logger.error(f"Error writing message: {e}")
            return False

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
        with self._db_lock:
            for full_key, value, backend in batch:
                self._write_to_backend(full_key, value, backend)

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

    def tether(self, **kwargs):
        """
        Expose the tether decorator with backend validation.

        :param kwargs: Arguments for the tether decorator.
        """
        backend = kwargs.get("backend", "local")
        self._validate_backend(backend)
        return tether(self, **kwargs)
