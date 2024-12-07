import dbm
import uuid
import json
import threading
import logging
from functools import wraps
from queue import Queue, Empty
import ssl
from typing import Union, Callable

import boto3
import etcd3gw


class DB:
    __slots__ = [
        "debug", "_db_lock", "_write_queue", "_running", "_worker_thread",
        "local_db_file", "dynamodb_table", "etcd", "batch_size", "batch_timeout", "logger"
    ]

    def __init__(self, config_file: str):
        """
        Initializes the DB class using a configuration file.

        :param config_file: Path to the JSON configuration file.
        """
        with open(config_file, "r", encoding="utf-8") as f:
            config = json.load(f)

        self.debug = config.get("debug", False)
        self._db_lock = threading.Lock()
        self._write_queue = Queue()
        self._running = False  # Indicates if the worker is running
        self._worker_thread = None

        # Batch configuration
        self.batch_size = config.get("queue_batch", {}).get("size", 10)
        self.batch_timeout = config.get("queue_batch", {}).get("timeout", 2.0)

        # Logger setup
        logging.basicConfig(level=logging.DEBUG if self.debug else logging.INFO)
        self.logger = logging.getLogger(__name__)

        # Initialize available backends
        self._initialize_backends(config)

    def _initialize_backends(self, config):
        # Sqlite3 local backend
        if "local" in config:
            self.local_db_file = config["local"]["filepath"]
            self.logger.debug(f"Local backend file path set: {self.local_db_file}")

        # DynamoDB backend
        dynamo_cfg = config.get("dynamodb", {})
        if "table_name" in dynamo_cfg:
            try:
                self.dynamodb_table = boto3.resource("dynamodb").Table(dynamo_cfg["table_name"])
                self.logger.debug("DynamoDB backend initialized.")
            except Exception as e:
                self.logger.error(f"Error initializing DynamoDB backend: {e}")
                self.dynamodb_table = None

        # etcd backend
        etcd_cfg = config.get("etcd", {})
        if "host" in etcd_cfg and "port" in etcd_cfg:
            try:
                ssl_context = None
                if etcd_cfg.get("use_ssl"):
                    ssl_context = ssl.create_default_context(cafile=etcd_cfg.get("ca_cert_file"))
                    ssl_context.load_cert_chain(
                        certfile=etcd_cfg.get("cert_file"),
                        keyfile=etcd_cfg.get("key_file")
                    )
                self.etcd = etcd3gw.client.Etcd3Client(
                    host=etcd_cfg["host"],
                    port=etcd_cfg["port"],
                    user=etcd_cfg.get("username"),
                    password=etcd_cfg.get("password"),
                    timeout=etcd_cfg.get("timeout", 5),
                    ssl_context=ssl_context
                )
                self.logger.debug("etcd backend initialized with advanced config.")
            except Exception as e:
                self.logger.error(f"Error initializing etcd backend: {e}")
                self.etcd = None

    def start(self):
        """Starts the background worker for queue writes."""
        if not self._running:
            self._running = True
            self._worker_thread = threading.Thread(
                target=self._background_worker,
                args=(self.batch_size, self.batch_timeout),
                daemon=True
            )
            self._worker_thread.start()
            self.logger.debug("Background worker started.")

    def stop(self):
        """Stops the background worker and gracefully shuts down resources."""
        if self._running:
            self._running = False
            self._write_queue.put(None)  # Signal the worker to stop
            self._worker_thread.join()  # Wait for the worker to stop
            self.logger.debug("Background worker stopped.")

    def _background_worker(self, batch_size: int = 10, batch_timeout: float = 2.0):
        """Worker to write queued messages in batches."""
        batch = []
        while self._running:
            try:
                item = self._write_queue.get(timeout=batch_timeout)
                if item is None:  # Graceful shutdown signal
                    break
                batch.append(item)
                self._write_queue.task_done()

                if len(batch) >= batch_size:
                    self._process_batch(batch)
                    batch = []  # Clear the batch
            except Empty:
                if batch:
                    self._process_batch(batch)
                    batch = []
            except Exception as e:
                self.logger.error(f"Error in background worker: {e}")

        # Final flush of remaining messages
        if batch:
            self._process_batch(batch)
        self.logger.debug("Background worker shutting down.")

    def _process_batch(self, batch: list):
        """Processes a batch of messages and writes them to the appropriate backends."""
        try:
            with self._db_lock:
                for full_key, value, backend in batch:
                    if backend == "local" and self.local_db_file is not None:
                        with dbm.open(self.local_db_file, "c") as db:
                            db[full_key] = value.encode("utf-8")
                            self.logger.debug(f"Batch write to local: {full_key} -> {value}")
                    elif backend == "dynamodb" and self.dynamodb_table is not None:
                        self.dynamodb_table.put_item(Item={"key": full_key, "value": value})
                        self.logger.debug(f"Batch write to DynamoDB: {full_key}")
                    elif backend == "etcd" and self.etcd is not None:
                        self.etcd.put(full_key, value)
                        self.logger.debug(f"Batch write to etcd: {full_key}")
                    else:
                        self.logger.error(f"Skipping invalid or uninitialized backend: {backend}")
        except Exception as e:
            self.logger.error(f"Error processing batch: {e}")


    def tether(self, bucket: str = "", wait: bool = True, backend: str = "local"):
        """
        A decorator to write the return value of a function to the database.

        The decorated function must return a dictionary with:
        - "key": Optional custom key (str).
        - "value": Data to store (str or dict).

        If "key" is not provided, a UUID is generated.

        :param bucket: Optional bucket prefix for logical grouping.
        :param wait: If True, writes immediately; if False, queues the write.
        :param backend: Backend to write to: 'local', 'dynamodb', or 'etcd'.
        """
        def decorator(func: Callable):
            @wraps(func)
            def wrapper(*args, **kwargs):
                result = func(*args, **kwargs)

                if isinstance(result, dict) and "value" in result:
                    key = result.get("key", str(uuid.uuid4()))
                    value = result["value"]

                    # Validate the value type
                    if not isinstance(value, (str, dict)):
                        self.logger.error(
                            f"Invalid 'value' type returned by '{func.__name__}'. "
                            f"Expected str or dict, got {type(value).__name__}."
                        )
                        return False

                    # Perform the write operation
                    if wait:
                        return self.write_message(key, value, bucket=bucket, backend=backend)
                    else:
                        self.write_queue_message(key, value, bucket=bucket, backend=backend)
                        return True
                else:
                    self.logger.error(
                        f"Function '{func.__name__}' must return a dict with 'value'."
                    )
                    return False
            return wrapper
        return decorator

    def write_message(self, key: str, value: Union[dict, str], bucket: str = "", backend: str = "local") -> bool:
        """
        Write a message to the specified backend.

        :param key: The key under which to store the message.
        :param value: The message to store, as a string or dictionary.
        :param bucket: Optional bucket or namespace prefix for the key.
        :param backend: The backend to write to (default: 'local').
        :return: True if the write is successful, False otherwise.
        """
        full_key = self._build_key(bucket, key)
        try:
            if isinstance(value, dict):
                value = json.dumps(value)

            with self._db_lock:
                if backend == "local" and self.local_db_file is not None:
                    with dbm.open(self.local_db_file, "c") as db:
                        db[full_key] = value.encode("utf-8")
                        self.logger.debug(f"Write successful to local backend: {full_key} -> {value}")
                elif backend == "dynamodb" and self.dynamodb_table is not None:
                    self.dynamodb_table.put_item(Item={"key": full_key, "value": value})
                    self.logger.debug(f"Write successful to DynamoDB: {full_key}")
                elif backend == "etcd" and self.etcd is not None:
                    self.etcd.put(full_key, value)
                    self.logger.debug(f"Write successful to etcd: {full_key}")
                else:
                    raise ValueError(f"Invalid backend or backend not initialized: {backend}")
            return True
        except Exception as e:
            self.logger.error(f"Error writing message to {backend}: {e}")
            return False

    def _build_key(self, key: str, bucket: str = "") -> str:
        return f"{bucket}:{key}" if bucket else key