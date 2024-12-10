
<div style="display: flex; align-items: flex-end;">
  <img src="assets/TDB_logo.png" alt="Logo" width="250" style="margin-right: 10px;">
</div>
<br>

# TetherDB - A Hybrid Key-Value Store for Local, DynamoDB, and etcd Backends

TetherDB is a flexible key-value store supporting **local storage**, **AWS DynamoDB**, and **etcd** backends. It simplifies data storage, batch processing, and retrieval with features like queued writes, bucketed organization, and key listing with pagination.

---

## **Key Features**

- **Hybrid Storage**:
  - Local storage (`dbm`).
  - AWS DynamoDB for scalable cloud storage.
  - etcd for distributed key-value management.

- **Direct and Queued Writes**:
  - Direct writes save immediately (overwriting existing keys).
  - Queued writes optimize for batch processing with configurable batch size and interval.

- **Batch Processing**:
  - Configurable batch size and processing interval ensure efficient queued writes.

- **Message Listing**:
  - Retrieve and paginate keys with optional prefix-based filtering.

- **`tether` Decorator**:
  - Simplify function integration by writing return values automatically to the database.

- **Thread-Safe Design**:
  - Queued writes and batch processing are thread-safe, ensuring concurrency.

- **Concurrent Writes**:
  - Simulate concurrent writes and stress-test backend performance.

- **Easy Configuration**:
  - Pass configuration as a JSON file or Python dictionary.

---

## **Installation**

### **Prerequisites**

- **Python 3.11+**
- **`boto3`**: For AWS DynamoDB integration.
- **`etcd3gw`**: For etcd support.

Install dependencies:

```sh
pip install boto3 etcd3gw
```

---

## **Configuration**

TetherDB can be configured using either a **JSON file** or a **Python dictionary**.

### **Example `config.json`**

```json
{
  "logging": "debug",
  "queue_batch": {
    "size": 10,
    "interval": 2.0
  },
  "local": {
    "filepath": "localdb"
  },
  "dynamodb": {
    "table_name": "MyDynamoDBTable"
  },
  "etcd": {
    "host": "localhost",
    "port": 2379,
    "use_ssl": true,
    "cert_file": "cert.pem",
    "key_file": "key.pem",
    "ca_cert_file": "ca.pem",
    "timeout": 5
  }
}
```

### **Python Configuration Dictionary**

```python
config = {
    "logging": "debug",
    "queue_batch": {"size": 10, "interval": 2.0},
    "local": {"filepath": "localdb"},
    "dynamodb": {"table_name": "MyDynamoDBTable"},
    "etcd": {
        "host": "localhost",
        "port": 2379,
        "use_ssl": True,
        "cert_file": "cert.pem",
        "key_file": "key.pem",
        "ca_cert_file": "ca.pem",
        "timeout": 5
    }
}
```

---

## **Usage**

### **Initialize TetherDB**

You can initialize the database by passing either a config file or a config dictionary.

```python
from TetherDB import DB

# Initialize with a config file
db = DB(config_file="config.json")

# Initialize with a Python dictionary
db = DB(config=config)
```

---

### **Methods**

#### **`write_message`**

Write a key-value pair **immediately** (overwriting existing keys) or **queue** it for background processing.

```python
db.write_message("key1", {"name": "Alice"}, backend="local")
db.write_message("key2", "simple_value", backend="etcd", queue=True)
```

**Parameters**:

| Parameter   | Type         | Description                                       |
|-------------|--------------|---------------------------------------------------|
| `key`       | `str`        | Key for the data.                                 |
| `value`     | `dict`/`str` | Data to store. If `dict`, it is JSON-encoded.     |
| `bucket`    | `str`        | Optional bucket prefix for logical grouping.      |
| `backend`   | `str`        | Backend to use: `local`, `dynamodb`, or `etcd`.   |
| `queue`     | `bool`       | Queue for background write (default: `False`).    |

---

#### **`update_message`**

Update an existing key-value pair. If the key does not exist, the update will fail.

```python
# Successful update
db.write_message("existing_key", {"data": "value"}, backend="local")
success = db.update_message("existing_key", {"data": "updated_value"}, backend="local")
print(success)  # True
```

**Parameters**:

| Parameter   | Type         | Description                                       |
|-------------|--------------|---------------------------------------------------|
| `key`       | `str`        | Key to update.                                    |
| `value`     | `dict`/`str` | New value to update. If `dict`, JSON-encoded.     |
| `bucket`    | `str`        | Optional bucket prefix for logical grouping.      |
| `backend`   | `str`        | Backend to use: `local`, `dynamodb`, or `etcd`.   |

**Returns**:

- `bool`: `True` if the update succeeds, `False` otherwise.

---

#### **`read_message`**

Retrieve a value by its key from the specified backend.

```python
value = db.read_message("key1", backend="local")
print(value)  # Output: {"name": "Alice"}
```

**Parameters**:

| Parameter  | Type   | Description                                  |
|------------|--------|----------------------------------------------|
| `key`      | `str`  | Key of the data to retrieve.                 |
| `bucket`   | `str`  | Optional bucket prefix for logical grouping. |
| `backend`  | `str`  | Backend to use: `local`, `dynamodb`, or `etcd`. |

**Returns**:

- The value stored under the specified key. If the key does not exist, it returns `None`.

---

#### **`list_messages`**

List messages with optional pagination and prefix filtering.

**Parameters**:

| Parameter      | Type     | Description                                        |
|-----------------|----------|----------------------------------------------------|
| `bucket`       | `str`    | Optional bucket prefix for filtering keys.         |
| `page_size`    | `int`    | Maximum number of keys per page.                   |
| `start_after`  | `str`    | Start listing keys after this key.                 |
| `backend`      | `str`    | Backend to list keys from: `local`, `dynamodb`, `etcd`. |

**Returns**:

```python
{"messages": ["value1", "value2"], "next_marker": "key2"}
```

---

#### **`tether`** Decorator

The `tether` decorator simplifies function integration by automatically writing the function's return value to the database. The return value must be a dictionary containing:

- `key`: The key for storing the value (a UUID is generated if omitted).
- `value`: The data to store.

```python
@db.tether(bucket="logs", backend="local", queue=True)
def generate_log():
    return {"key": "log:001", "value": {"event": "UserLogin", "status": "Success"}}

generate_log()
```

In this example:

- A log with key `log:001` is written to the `local` backend.
- The `queue=True` argument ensures the write is processed asynchronously.

**Parameters**:

| Parameter   | Type    | Description                                       |
|-------------|---------|---------------------------------------------------|
| `bucket`    | `str`   | Optional bucket prefix for logical grouping.      |
| `queue`     | `bool`  | If `True`, the write will be queued. Default `False`. |
| `backend`   | `str`   | Backend to write to: `local`, `dynamodb`, or `etcd`. |

**Return**:

- Writes the return value of the function to the database and returns `True` on success.

---

## **Debugging and Logging**

Enable debug logging by setting `"logging": "debug"` in the config.

---

## **Lifecycle Management**

```python
db.stop()
```

Ensures pending writes are completed before shutdown.

---

## **Conclusion**

TetherDB simplifies key-value storage across local, cloud, and distributed backends. Its features include batch processing, decorators, and robust thread safety, making it ideal for scalable and concurrent applications.
