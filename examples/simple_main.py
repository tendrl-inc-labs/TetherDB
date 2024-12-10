from TetherDB import DB
import time


def verify_write(db, key, backend):
    """
    Verify that a key exists in the specified backend.
    """
    value = db.read_message(key, backend=backend)
    if value:
        print(f"✔ Verified: '{key}' in '{backend}' with value: {value}")
    else:
        print(f"❌ ERROR: Key '{key}' not found in '{backend}'.")


def test_update_message(db, backend, key, initial_value, updated_value):
    """
    Test updating a message in the backend.
    """
    db.write_message(key, initial_value, backend=backend)
    print(f"✔ Initial write: '{key}' -> {initial_value} in '{backend}'.")

    success = db.update_message(key, updated_value, backend=backend)
    if success:
        print(f"✔ Update successful: '{key}' -> {updated_value} in '{backend}'.")
        verify_write(db, key, backend)
    else:
        print(f"❌ ERROR: Update failed for '{key}' in '{backend}'.")


def test_backend_operations(db, backend, direct_key, update_key, queued_key):
    """
    Test basic operations for a specific backend.
    """
    print(f"\n== Testing '{backend}' Backend ==")

    # Direct Write and Verify
    db.write_message(direct_key, {"data": f"{backend}_direct_write"}, backend=backend)
    verify_write(db, direct_key, backend)

    # Update and Verify
    test_update_message(
        db,
        backend,
        update_key,
        {"data": "initial_value"},
        {"data": "updated_value"},
    )

    # Queued Write and Verify
    print(f"Enqueuing queued write to '{backend}'...")
    db.write_message(
        queued_key, {"data": f"{backend}_queued_write"}, backend=backend, queue=True
    )
    time.sleep(2)  # Allow queued writes to process
    verify_write(db, queued_key, backend)


def main():
    # Path to the config file
    config_file = "examples/config.json"

    # Initialize the DB instance
    db = DB(config_file=config_file)

    # ================================
    # Local Backend
    # ================================
    test_backend_operations(
        db,
        backend="local",
        direct_key="local_direct",
        update_key="local_update",
        queued_key="local_queued",
    )

    # ================================
    # DynamoDB Backend
    # ================================
    test_backend_operations(
        db,
        backend="dynamodb",
        direct_key="dynamo_direct",
        update_key="dynamo_update",
        queued_key="dynamo_queued",
    )

    # ================================
    # etcd Backend
    # ================================
    test_backend_operations(
        db,
        backend="etcd",
        direct_key="etcd_direct",
        update_key="etcd_update",
        queued_key="etcd_queued",
    )

    # Shutdown worker
    print("\nShutting down background worker...")
    db.stop()
    print("✔ All tests completed successfully.")


if __name__ == "__main__":
    main()
