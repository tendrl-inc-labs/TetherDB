import uuid
from functools import wraps

def tether(db_instance, bucket: str = "", wait: bool = True, backend: str = "local"):
    """
    A decorator to write the return value of a function to the database.

    The function must return a dictionary with:
    - "key": Optional custom key (str). A UUID is generated if not provided.
    - "value": The data to store (str or dict).

    :param db_instance: Reference to the DB instance.
    :param bucket: Optional bucket prefix.
    :param wait: If True, writes immediately; if False, queues the write.
    :param backend: Backend to write to: 'local', 'dynamodb', or 'etcd'.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if isinstance(result, dict) and "value" in result:
                key = result.get("key", str(uuid.uuid4()))
                db_instance.write_message(key, result["value"], bucket, backend, not wait)
            else:
                db_instance.logger.error(
                    f"Function '{func.__name__}' must return a dictionary with 'value'."
                )
        return wrapper
    return decorator