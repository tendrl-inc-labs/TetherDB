import dbm
import json
from logging import Logger
import boto3
from etcd3gw.client import Etcd3Client


class BackendInitializer:
    """
    Handles initialization and operations for the supported backends: local, DynamoDB, and etcd.
    """

    def __init__(self, config: dict, logger: Logger):
        self.logger = logger
        self.local_db_file = None
        self.dynamodb_table = None
        self.etcd = None

        self._init_local(config)
        self._init_dynamodb(config)
        self._init_etcd(config)

    def _init_local(self, config: dict):
        """Initialize the Local backend."""
        if "local" in config:
            self.local_db_file = config["local"]["filepath"]
            self.logger.debug(f"Local backend initialized at {self.local_db_file}")

    def _init_dynamodb(self, config: dict):
        """Initialize the DynamoDB backend."""
        try:
            dynamo_cfg = config.get("dynamodb", {})
            if "table_name" in dynamo_cfg:
                self.dynamodb_table = boto3.resource("dynamodb").Table(
                    dynamo_cfg["table_name"]
                )
                self.logger.debug("DynamoDB backend initialized.")
        except Exception as e:
            self.logger.error(f"Error initializing DynamoDB backend: {e}")

    def _init_etcd(self, config: dict):
        """Initialize the etcd backend."""
        try:
            etcd_cfg = config.get("etcd", {})
            host = etcd_cfg.get("host", "localhost")
            port = etcd_cfg.get("port", 2379)
            timeout = etcd_cfg.get("timeout", 5)

            self.etcd = Etcd3Client(host=host, port=port, timeout=timeout)
            self.logger.debug("etcd backend initialized.")
        except Exception as e:
            self.logger.error(f"Error initializing etcd backend: {e}")

    def write(self, key: str, value: str, backend: str):
        """Write data to the specified backend."""
        if backend == "local":
            with dbm.open(self.local_db_file, "c") as db:
                db[key] = value
        elif backend == "dynamodb":
            self.dynamodb_table.put_item(Item={"key": key, "value": value})
        elif backend == "etcd":
            self.etcd.put(key, value)
        else:
            raise ValueError(f"Unsupported backend: {backend}")

    def read(self, key: str, backend: str):
        """Read data from the specified backend."""
        if backend == "local":
            with dbm.open(self.local_db_file, "r") as db:
                value = db.get(key)
                if value:
                    return self._deserialize(value.decode("utf-8"))
                return None
        elif backend == "dynamodb":
            response = self.dynamodb_table.get_item(Key={"key": key})
            value = response.get("Item", {}).get("value")
            if value:
                return self._deserialize(value)
            return None
        elif backend == "etcd":
            response = self.etcd.get(key)
            if response:
                for raw_value in response:
                    if isinstance(raw_value, bytes):
                        return self._deserialize(raw_value.decode("utf-8"))
            return None
        else:
            raise ValueError(f"Unsupported backend: {backend}")

    def update(self, key: str, value: str, backend: str):
        """Update a key's value if it exists in the specified backend."""
        if backend == "local":
            return self._update_local(key, value)
        elif backend == "dynamodb":
            return self._update_dynamodb(key, value)
        elif backend == "etcd":
            return self._update_etcd(key, value)
        else:
            raise ValueError(f"Unsupported backend: {backend}")

    def _update_local(self, key, value):
        """Update a key in the Local backend."""
        with dbm.open(self.local_db_file, "c") as db:
            if key.encode() in db:
                db[key] = value
                return True
            return False

    def _update_dynamodb(self, key, value):
        """Update a key in the DynamoDB backend."""
        response = self.dynamodb_table.update_item(
            Key={"key": key},
            UpdateExpression="SET #v = :val",
            ExpressionAttributeNames={"#v": "value"},
            ExpressionAttributeValues={":val": value},
            ReturnValues="UPDATED_NEW",
        )
        return "Attributes" in response

    def _update_etcd(self, key, value):
        """Update a key in the etcd backend."""
        existing_value = self.etcd.get(key)
        if existing_value:  # Ensure the response is valid
            for raw_value in existing_value:  # Iterate over response values
                if isinstance(raw_value, bytes):  # Check if value is bytes
                    self.etcd.put(key, value)  # Perform the update
                    return True
        return False

    def list_messages(self, page_size, start_after, bucket, backend):
        """List values (messages) for the specified backend."""
        if backend == "local":
            return self._list_messages_local(page_size, start_after, bucket)
        elif backend == "dynamodb":
            return self._list_messages_dynamodb(page_size, start_after, bucket)
        elif backend == "etcd":
            return self._list_messages_etcd(page_size, start_after, bucket)
        else:
            raise ValueError(f"Unsupported backend: {backend}")

    def _list_messages_local(self, page_size, start_after, bucket):
        """List messages for the Local backend."""
        messages = []
        with dbm.open(self.local_db_file, "r") as db:
            keys = sorted(k.decode("utf-8") for k in db.keys())
            if bucket:
                keys = [k for k in keys if k.startswith(bucket)]

            start_index = 0
            if start_after:
                start_index = keys.index(start_after) + 1 if start_after in keys else 0
            paginated_keys = keys[start_index : start_index + page_size]

            for k in paginated_keys:
                raw_value = db.get(k.encode("utf-8"))
                if raw_value:
                    messages.append(self._deserialize(raw_value.decode("utf-8")))

        return {
            "messages": messages,
            "next_marker": paginated_keys[-1] if paginated_keys else None,
        }

    def _list_messages_dynamodb(self, page_size, start_after, bucket):
        """List messages for the DynamoDB backend."""
        scan_args = {"Limit": page_size}
        if start_after:
            scan_args["ExclusiveStartKey"] = {"key": start_after}

        response = self.dynamodb_table.meta.client.scan(**scan_args)
        items = response.get("Items", [])
        messages = [self._deserialize(item.get("value", "")) for item in items]

        next_marker = response.get("LastEvaluatedKey", None)
        if next_marker:
            next_marker = next_marker.get("key")

        return {"messages": messages, "next_marker": next_marker}

    def _list_messages_etcd(self, page_size, start_after, bucket):
        """List messages for the etcd backend."""
        key_prefix = bucket or ""
        if start_after:
            key_prefix += start_after + "\0"

        response = self.etcd.get_prefix(key_prefix)
        messages = [
            self._deserialize(kv.value.decode("utf-8"))
            for kv in response.kvs[:page_size]
        ]

        next_marker = (
            response.kvs[-1].key.decode("utf-8")
            if len(response.kvs) == page_size
            else None
        )
        return {"messages": messages, "next_marker": next_marker}

    @staticmethod
    def _deserialize(value: str):
        """Deserialize a value into its proper Python type."""
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
