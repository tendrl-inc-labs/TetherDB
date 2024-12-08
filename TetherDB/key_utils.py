def build_key(bucket: str, key: str) -> str:
    """
    Builds a key with an optional bucket prefix.
    """
    return f"{bucket}:{key}" if bucket else key

def paginate_keys(keys: list, page_size: int, start_after: str = None) -> tuple:
    """
    Paginates a list of keys.

    :param keys: List of keys.
    :param page_size: Number of keys per page.
    :param start_after: Starting point for pagination.
    """
    start_index = 0
    if start_after:
        start_index = keys.index(start_after) + 1 if start_after in keys else 0
    paginated_keys = keys[start_index:start_index + page_size]
    next_marker = paginated_keys[-1] if len(paginated_keys) == page_size else None
    return paginated_keys, next_marker