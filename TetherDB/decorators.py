import uuid
import json
from functools import wraps



def tether(db_instance, bucket: str = "", queue: bool = False, backend: str = "local"):
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
                value = result["value"]
                if isinstance(value, dict):
                    value = json.dumps(value)
                db_instance.write_message(key, value, bucket, backend, not queue)
                return True
            else:
                raise ValueError(
                    "Function return value must be a dictionary containing a 'value' key."
                )

        return wrapper

    return decorator
