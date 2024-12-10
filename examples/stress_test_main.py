from TetherDB import DB
import time
import threading

# ================================
# Utility Functions for Robust Testing
# ================================


def concurrent_writes(db, backend, prefix):
    """
    Simulate concurrent writes to test multi-threaded behavior.
    """
    threads = []
    for i in range(10):  # Simulate 10 concurrent threads
        t = threading.Thread(
            target=db.write_message,
            args=(f"{prefix}_key_{i}", {"data": f"value_{i}"}, "", backend),
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    print(f"Concurrent writes to {backend} completed.")


def stress_test(db, backend, num_writes=100):
    """
    Perform a stress test by writing a large number of keys to a backend.
    """
    print(f"Stress Testing {backend}: Writing {num_writes} keys...")
    try:
        for i in range(num_writes):
            db.write_message(
                f"stress_key_{i}", {"data": f"value_{i}"}, backend=backend, queue=True
            )
        time.sleep(3)  # Allow queued writes to process
        result = db.list_messages(page_size=num_writes, backend=backend)
        print(f"Total keys written: {len(result['messages'])}")
    except Exception as e:
        print(f"ERROR during stress test on {backend}: {str(e)}")


def verify_write(db, key, backend):
    """
    Verify that a key exists in the specified backend.
    """
    value = db.read_message(key, backend=backend)
    if value:
        print(f"Verified key '{key}' in backend '{backend}': {value}")
    else:
        print(f"ERROR: Key '{key}' not found in backend '{backend}'.")


def test_update_message(db, backend, key, initial_value, updated_value):
    """
    Test updating a message in the backend.
    """
    db.write_message(key, initial_value, backend=backend)
    print(f"Initial write complete for '{key}' in '{backend}'.")

    success = db.update_message(key, updated_value, backend=backend)
    if success:
        print(
            f"Successfully updated key '{key}' to '{updated_value}' in backend '{backend}'."
        )
        value = db.read_message(key, backend=backend)
        print(f"Verified updated value: {value}")
    else:
        print(f"ERROR: Update failed for key '{key}' in backend '{backend}'.")


# ================================
# Main Testing Script
# ================================
def main():
    # Path to the config file
    config_file = "examples/config.json"

    # Initialize the DB instance
    db = DB(config_file=config_file)

    # ================================
    # Local Backend Operations
    # ================================
    print("== Local Backend Operations ==")

    # Direct Write and Verify
    db.write_message("local_key1", {"data": "local_direct_write"}, backend="local")
    verify_write(db, "local_key1", "local")

    # Update and Verify
    test_update_message(
        db,
        "local",
        "local_update_key",
        {"data": "initial_value"},
        {"data": "updated_value"},
    )

    # Queued Write
    db.write_message(
        "local_key2", {"data": "local_queued_write"}, backend="local", queue=True
    )
    print("Queued write to Local backend enqueued.")
    time.sleep(3)  # Wait for processing
    verify_write(db, "local_key2", "local")

    # ================================
    # DynamoDB Backend Operations
    # ================================
    print("\n== DynamoDB Backend Operations ==")

    # Direct Write and Verify
    db.write_message("dynamo_key1", {"data": "dynamo_direct_write"}, backend="dynamodb")
    verify_write(db, "dynamo_key1", "dynamodb")

    # Update and Verify
    test_update_message(
        db,
        "dynamodb",
        "dynamo_update_key",
        {"data": "initial_value"},
        {"data": "updated_value"},
    )

    # Queued Write
    db.write_message(
        "dynamo_key2", {"data": "dynamo_queued_write"}, backend="dynamodb", queue=True
    )
    print("Queued write to DynamoDB backend enqueued.")
    time.sleep(3)  # Wait for processing
    verify_write(db, "dynamo_key2", "dynamodb")

    # ================================
    # etcd Backend Operations
    # ================================
    print("\n== etcd Backend Operations ==")

    # Direct Write and Verify
    db.write_message("etcd_key1", {"data": "etcd_direct_write"}, backend="etcd")
    verify_write(db, "etcd_key1", "etcd")

    # Update and Verify
    test_update_message(
        db,
        "etcd",
        "etcd_update_key",
        {"data": "initial_value"},
        {"data": "updated_value"},
    )

    # Queued Write
    db.write_message(
        "etcd_key2", {"data": "etcd_queued_write"}, backend="etcd", queue=True
    )
    print("Queued write to etcd backend enqueued.")
    time.sleep(3)  # Wait for processing
    verify_write(db, "etcd_key2", "etcd")

    # ================================
    # Shutdown
    # ================================
    print("\nShutting down the background worker...")
    db.stop()
    print("Done!")


if __name__ == "__main__":
    main()
