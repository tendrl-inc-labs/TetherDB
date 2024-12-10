def build_key(bucket: str, key: str) -> str:
    """
    Builds a composite key with an optional bucket prefix.

    This function combines a bucket name (if provided) with the key to create a unique,
    prefixed key for storage. If no bucket is provided, the original key is returned.

    :param bucket: Optional bucket name (prefix) for the key.
    :param key: The base key name.
    :return: A string representing the full key with the bucket prefix (if provided).
    """
    return f"{bucket}:{key}" if bucket else key


def paginate_keys(
    keys: list[str], page_size: int, start_after: str = None
) -> tuple[list[str], str | None]:
    """
    Paginates a list of keys for consistent and controlled key retrieval.

    This function supports:
      - Dividing a list of keys into pages.
      - Returning a subset of keys starting after a specific key (for pagination).
      - Returning a "next_marker" to indicate the starting point for the next page.

    :param keys: List of keys to paginate (must be strings).
    :param page_size: Maximum number of keys to include in the page.
    :param start_after: Optional key indicating where to start the page (exclusive).
        If the key is not found, pagination starts from the beginning.
    :return: A tuple containing:
        - paginated_keys: List of keys in the current page.
        - next_marker: The last key in the current page, or None if no more pages exist.

    Example:
        keys = ["a", "b", "c", "d"]
        paginate_keys(keys, page_size=2) -> (["a", "b"], "b")
        paginate_keys(keys, page_size=2, start_after="b") -> (["c", "d"], None)
        paginate_keys(keys, page_size=3, start_after="c") -> (["d"], None)
    """
    # Determine the starting index for pagination
    start_index = 0
    if start_after:
        start_index = keys.index(start_after) + 1 if start_after in keys else 0

    # Slice the keys list to get the current page
    paginated_keys = keys[start_index : start_index + page_size]

    # Determine the next marker (last key of the current page)
    next_marker = paginated_keys[-1] if len(paginated_keys) == page_size else None

    return paginated_keys, next_marker
