<div style="display: flex; align-items: flex-end;">
  <img src="TDB_logo.png" alt="Logo" width="250" style="margin-right: 10px;">
</div>
<br>

# A Hybrid Key-Value Store for Local, DynamoDB, and etcd Backends

## TetherDB is a flexible key-value store supporting

1. **Local storage** using `dbm` (SQLite3-backed in Python 3.13+).
2. **AWS DynamoDB** for scalable cloud storage.
3. **etcd** for distributed key-value storage with authentication and TLS support.

---

## Key Features

- **Hybrid Storage**: Supports `local`, `DynamoDB`, or `etcd` backends.
- **Direct Writes or Queued Writes**:
  - Direct writes for immediate storage.
  - Queued writes for efficient batch processing in the background.
- **Batch Processing**: Configurable batch size and timeouts for queued writes.
- **Logging Configuration**:
  - Control logging levels (`debug`, `info`, `none`) directly via `config.json`.
- **Lifecycle Management**:
  - **`start()`**: Start background workers for queued writes.
  - **`stop()`**: Gracefully stop the background worker.
- **Buckets**: Organize keys logically using prefixes.
- **`tether` Decorator**: Automatically write function outputs to the database.
- **JSON Configuration**: Simplify backend setup and logging controls.

---

## Installation

### Prerequisites

- **Python 3.13+** (for SQLite3-backed `dbm`).
- **`boto3`**: For AWS DynamoDB integration.
- **`etcd3gw`**: For etcd support.

Install dependencies:

```bash
pip install boto3 etcd3gw
```

---

## Configuration

Create a `config.json` file to specify backend settings and logging levels.

### Example `config.json`

```json
{
  "logging": "debug",  // Options: debug, info, none
  "queue_batch": {
    "size": 10,
    "timeout": 2.0
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
    "username": "user",
    "password": "password",
    "use_ssl": true,
    "cert_file": "cert.pem",
    "key_file": "key.pem",
    "ca_cert_file": "ca.pem",
    "timeout": 5
  }
}
```

### Logging Configuration

Set the `logging` level in the `config.json` file:

- **`debug`**: Detailed debug information.
- **`info`**: High-level logs only.
- **`none`**: Disable all logging.

---

## Initialization

```python
from TetherDB import DB

db = DB("config.json")
```

---

## Methods Overview

### 1. **`write_message`**

Write a key-value pair **immediately** to the specified backend.

```python
db.write_message("key1", {"name": "Alice"}, backend="local")
```

| Parameter  | Type         | Description                                        |
|------------|--------------|----------------------------------------------------|
| `key`      | `str`        | Key for the data.                                  |
| `value`    | `dict`/`str` | Data to store. If `dict`, it is JSON-encoded.      |
| `bucket`   | `str`        | Optional bucket prefix for logical grouping.       |
| `backend`  | `str`        | Backend to use: `local`, `dynamodb`, or `etcd`.    |
| `queue`    | `bool`       | Queue for background write (default: `False`).     |

---

### 2. **`start`**

Start the background worker for queued writes.

```python
db.start()
```

---

### 3. **`stop`**

Stop the background worker and flush any remaining messages.

```python
db.stop()
```

---

### 4. **`tether` Decorator**

Automatically write the **return value** of a function to the database.

**Function Return Requirements**:

- The function must return a dictionary with:
  - `"key"`: Custom key (optional). A UUID is generated if not provided.
  - `"value"`: The data to store. Must be a `str` or `dict`.

```python
@db.tether(bucket="users", backend="local", wait=True)
def fetch_user():
    return {"key": "user:123", "value": {"name": "Alice", "role": "admin"}}

fetch_user()
```

---

## Queued Writes with Batch Processing

TetherDB supports background writes with batching. Configure these in `config.json`:

```json
"queue_batch": {
    "size": 5,
    "timeout": 2.0
}
```

**Example:**

```python
db.start()
db.write_message("key2", {"name": "Bob"}, backend="etcd", queue=True)
db.stop()
```

---

## Debug Mode

Enable `debug` logging in `config.json`:

```json
"logging": "debug"
```

Example debug output:

```
2024-06-01 10:00:00 - DEBUG - Message queued for write: key2 -> {"name": "Bob"}
2024-06-01 10:00:01 - DEBUG - Batch write to etcd: key2 -> {"name": "Bob"}
2024-06-01 10:00:02 - DEBUG - Background worker shutting down.
```

---

## Cleanup and Shutdown

Always stop the worker to ensure data integrity:

```python
db.stop()
```

---

## Closing Thoughts

TetherDB offers flexible and efficient key-value storage across `local`, `DynamoDB`, and `etcd` backends. It is designed for performance, scalability, and ease of use.

- **Direct writes** for simplicity.
- **Queued writes** for efficiency and batching.
- **Tether decorator** for seamless integration with function outputs.

Configure logging, choose your backend, and enjoy efficient storage management!
