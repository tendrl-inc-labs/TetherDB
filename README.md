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
  - Direct writes save immediately.
  - Queued writes optimize for batch processing with configurable batch size and interval.

- **Batch Processing**:
  - Configurable batch size and processing interval ensure efficient queued writes.

- **Key Listing**:
  - Retrieve and paginate keys with optional prefix-based filtering.

- **`tether` Decorator**:
  - Simplify function integration by writing return values automatically to the database.

- **Thread-Safe Design**:
  - Queued writes and batch processing are thread-safe, ensuring concurrency.

- **Easy Configuration**:
  - Pass configuration as a JSON file or Python dictionary.

---

## **Installation**

### **Prerequisites**

- **Python 3.13+** (for SQLite3-backed `dbm`).
- **`boto3`**: For AWS DynamoDB integration.
- **`etcd3gw`**: For etcd support.

Install dependencies:

```bash
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

#### 1. **`write_message`**

Write a key-value pair **immediately** or **queue** it for background processing.

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

#### 2. **`list_keys`**

List keys stored in a backend with optional pagination and prefix-based filtering.

```python
result = db.list_keys(page_size=5, backend="local")
print(result)  # {"keys": [...], "next_marker": ...}
```

**Parameters**:

| Parameter      | Type     | Description                                        |
|-----------------|----------|----------------------------------------------------|
| `bucket`       | `str`    | Optional bucket prefix for filtering keys.         |
| `page_size`    | `int`    | Maximum number of keys per page.                   |
| `start_after`  | `str`    | Start listing keys after this key.                 |
| `backend`      | `str`    | Backend to list keys from: `local`, `dynamodb`, `etcd`. |

**Returns**:

```json
{"keys": ["key1", "key2"], "next_marker": "key2"}
```

---

#### 3. **`start`** and **`stop`**

Control the background worker for queued writes.

```python
db.start()  # Start the background worker
db.stop()   # Gracefully stop the worker
```

---

#### 4. **`tether` Decorator**

The `tether` decorator allows you to automatically write the return value of a function to the database.

##### **Function Requirements**

- The function **must return** a dictionary containing:
  - **`key`**: (Optional) Custom key for the data. Defaults to a UUID.
  - **`value`**: The data to store, which can be a `dict` or a `str`.

##### **Example Usage**

```python
@db.tether(bucket="users", backend="local", queue=True)
def generate_user():
    return {"key": "user:123", "value": {"name": "Alice", "role": "admin"}}

generate_user()
```

---

## **Debugging and Logging**

Enable debug logging to monitor queued writes, batch processing, and errors. Set the logging level in the configuration:

```json
"logging": "debug"
```

Sample debug output:

```
2024-06-01 10:00:00 - DEBUG - Message queued: queued_key -> queued_write
2024-06-01 10:00:01 - DEBUG - Written to Local: queued_key
2024-06-01 10:00:02 - DEBUG - Background worker stopped.
```

---

## **Lifecycle Management**

To ensure all queued writes are processed before shutting down:

```python
db.stop()
```

---

## **Real-World Example**

```python
from TetherDB import DB

# Initialize the DB
db = DB(config_file="config.json")

# Direct write to Local backend
db.write_message("direct_key", {"message": "Hello World!"}, backend="local")

# Queued write to etcd backend
db.write_message("queued_key", "Queued Data", backend="etcd", queue=True)

# Use tether decorator
@db.tether(bucket="logs", backend="dynamodb", queue=False)
def generate_log():
    return {"key": "log:001", "value": {"event": "UserLogin", "status": "Success"}}

generate_log()

# List keys
print(db.list_keys(page_size=10, backend="local"))

# Stop worker
db.stop()
```

---

## **Conclusion**

TetherDB is a flexible, lightweight, and hybrid key-value store that adapts to various storage backends. Whether you need local, cloud-based, or distributed storage, TetherDB provides a seamless and efficient solution with robust features like batch processing, prefix-based keys, and automated decorators.
