<div style="display: flex; align-items: flex-end;">
  <img src="assets/TDB_logo.png" alt="Logo" width="250" style="margin-right: 10px;">
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
- **Flexible Configuration**:
  - Pass a configuration file (`config_file`) **or** a configuration dictionary (`config`).
  - Clear error handling ensures only one configuration method is used.
- **Logging Configuration**:
  - Control logging levels (`debug`, `info`, `none`) directly via `config`.
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

### DynamoDB Table Setup

To use the DynamoDB backend, you must create a DynamoDB table with the following requirements:

1. **Table Name**: Must match the `table_name` provided in the `config` or `config_file`.

2. **Primary Key**:
   - **Partition Key**: Use a string attribute named `key`.
   - **No Sort Key** is required.

3. **Example AWS CLI Command to Create the Table**:

```bash
aws dynamodb create-table \
    --table-name MyDynamoDBTable \
    --attribute-definitions AttributeName=key,AttributeType=S \
    --key-schema AttributeName=key,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST
```

### Table Schema Example

| Attribute Name | Type    | Key Type      |
|----------------|---------|---------------|
| `key`         | String  | Partition Key |

---

## DynamoDB Configuration in `config.json`

```json
"dynamodb": {
  "table_name": "MyDynamoDBTable"
}
```

Ensure the table exists and matches the configuration. If the table does not exist, DynamoDB operations will fail.

---

## Configuration

TetherDB supports configuration in two ways:

1. **Using a JSON file (`config_file`)**.
2. **Passing a Python dictionary (`config`)**.

> **Note**: You must provide **one**, not both. TetherDB will raise an error if both are passed.

---

### Example 1: JSON Configuration File (`config.json`)

Create a `config.json` file to specify backend settings and logging levels.

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

**Usage**:

```python
from TetherDB import DB

db = DB(config_file="config.json")
```

---

### Example 2: Python Configuration Dictionary (`config`)

```python
from TetherDB import DB

config = {
    "logging": "debug",
    "queue_batch": {"size": 10, "timeout": 2.0},
    "local": {"filepath": "localdb"},
    "dynamodb": {"table_name": "MyDynamoDBTable"},
    "etcd": {
        "host": "localhost",
        "port": 2379,
        "username": "user",
        "password": "password",
        "use_ssl": True,
        "cert_file": "cert.pem",
        "key_file": "key.pem",
        "ca_cert_file": "ca.pem",
        "timeout": 5
    }
}

db = DB(config=config)
```

---

## Methods Overview

### 1. **`write_message`**

Write a key-value pair **immediately** or **queue** it for background processing.

```python
db.write_message("key1", {"name": "Alice"}, backend="local")
db.write_message("key2", "simple_value", backend="etcd", queue=True)
```

| Parameter  | Type         | Description                                        |
|------------|--------------|----------------------------------------------------|
| `key`      | `str`        | Key for the data.                                  |
| `value`    | `dict`/`str` | Data to store. If `dict`, it is JSON-encoded.      |
| `bucket`   | `str`        | Optional bucket prefix for logical grouping.       |
| `backend`  | `str`        | Backend to use: `local`, `dynamodb`, or `etcd`.    |
| `queue`    | `bool`       | Queue for background write (default: `False`).     |

---

### 2. **`list_keys`**

List keys stored in a backend with optional pagination.

```python
result = db.list_keys(page_size=5, backend="local")
print(result)
```

| Parameter      | Type     | Description                                        |
|----------------|----------|----------------------------------------------------|
| `bucket`       | `str`    | Optional bucket prefix for filtering keys.         |
| `page_size`    | `int`    | Maximum number of keys per page.                   |
| `start_after`  | `str`    | Start listing keys after this key.                 |
| `backend`      | `str`    | Backend to list keys from: `local`, `dynamodb`, `etcd`. |

---

### 3. **`start`** and **`stop`**

Control the background worker for queued writes.

```python
db.start()  # Start the background worker
db.stop()   # Gracefully stop the worker
```

---

### 4. **`tether` Decorator**

Write the **return value** of a function to the database.

#### Requirements

- Function must return a dictionary with:
  - `"key"`: (Optional) Custom key. Defaults to a UUID if not provided.
  - `"value"`: The data to store (str or dict).

**Example**:

```python
@db.tether(bucket="users", backend="local", queue=True)
def generate_user():
    return {"key": "user:123", "value": {"name": "Alice", "role": "admin"}}

generate_user()
```

---

## Debug Mode

Enable `debug` logging in the configuration:

```json
"logging": "debug"
```

Sample Debug Output:

```
2024-06-01 10:00:00 - DEBUG - Message queued for write: queued_key -> queued_write
2024-06-01 10:00:01 - DEBUG - Batch write to etcd: queued_key -> queued_write
2024-06-01 10:00:02 - DEBUG - Background worker shutting down.
```

---

## Cleanup and Shutdown

Always stop the background worker to ensure no pending writes are lost:

```python
db.stop()
```

---

## Closing Thoughts

TetherDB offers:

- **Direct Writes** for simplicity.
- **Queued Writes** for efficient batching.
- **Tether Decorator** for seamless function integration.

Easily configure your preferred backend, enable logging, and enjoy the flexibility of hybrid storage!
