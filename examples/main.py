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
    for i in range(num_writes):
        db.write_message(
            f"stress_key_{i}", {"data": f"value_{i}"}, backend=backend, queue=True
        )
    time.sleep(3)  # Allow queued writes to process
    result = db.list_keys(backend=backend)
    print(f"Total keys written: {len(result['keys'])}")


def verify_write(db, key, backend):
    """
    Verify that a key exists in the specified backend.
    """
    result = db.list_keys(backend=backend)
    keys = result["keys"]
    if key in keys:
        print(f"Verified key '{key}' in backend '{backend}'.")
    else:
        print(f"ERROR: Key '{key}' not found in backend '{backend}'.")


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

    # Direct Write
    db.write_message("local_key1", {"data": "local_direct_write"}, backend="local")
    print("Direct write to Local backend successful.")
    verify_write(db, "local_key1", "local")

    # Queued Write
    db.write_message(
        "local_key2", {"data": "local_queued_write"}, backend="local", queue=True
    )
    print("Queued write to Local backend enqueued.")

    # Using Tether Decorator
    @db.tether(bucket="local_logs", queue=True, backend="local")
    def local_tether_example():
        return {"key": "local_tether_key", "value": "local_tethered_data"}

    local_tether_example()
    print("Tethered write to Local backend complete.")

    # Give time for queued writes to process
    time.sleep(3)

    # Retrieve keys from Local Backend
    print("Listing keys from Local backend:")
    local_keys = db.list_keys(backend="local")
    print(local_keys)

    # Test Pagination
    print("\nTesting Pagination for Local Backend:")
    for i in range(20):
        db.write_message(
            f"paginated_key_{i}", f"value_{i}", backend="local", queue=True
        )
    time.sleep(3)

    page_size = 5
    next_marker = None
    while True:
        result = db.list_keys(
            page_size=page_size, start_after=next_marker, backend="local"
        )
        keys, next_marker = result["keys"], result["next_marker"]
        print(f"Keys: {keys}")
        if not next_marker:
            break

    # Concurrent Writes and Stress Testing
    concurrent_writes(db, "local", "local_concurrent")
    stress_test(db, "local", num_writes=50)

    # ================================
    # DynamoDB Backend Operations
    # ================================
    print("\n== DynamoDB Backend Operations ==")

    # Direct Write
    db.write_message("dynamo_key1", {"data": "dynamo_direct_write"}, backend="dynamodb")
    print("Direct write to DynamoDB backend successful.")
    verify_write(db, "dynamo_key1", "dynamodb")

    # Queued Write
    db.write_message(
        "dynamo_key2", {"data": "dynamo_queued_write"}, backend="dynamodb", queue=True
    )
    print("Queued write to DynamoDB backend enqueued.")

    # Tether Decorator
    @db.tether(bucket="dynamo_logs", queue=True, backend="dynamodb")
    def dynamo_tether_example():
        return {"key": "dynamo_tether_key", "value": "dynamo_tethered_data"}

    dynamo_tether_example()
    print("Tethered write to DynamoDB backend complete.")

    # Wait for writes and list keys
    time.sleep(3)
    print("Listing keys from DynamoDB backend:")
    dynamo_keys = db.list_keys(backend="dynamodb")
    print(dynamo_keys)

    # Stress Test
    stress_test(db, "dynamodb", num_writes=20)

    # ================================
    # etcd Backend Operations
    # ================================
    print("\n== etcd Backend Operations ==")

    # Direct Write
    db.write_message("etcd_key1", {"data": "etcd_direct_write"}, backend="etcd")
    print("Direct write to etcd backend successful.")
    verify_write(db, "etcd_key1", "etcd")

    # Queued Write
    db.write_message(
        "etcd_key2", {"data": "etcd_queued_write"}, backend="etcd", queue=True
    )
    print("Queued write to etcd backend enqueued.")

    # Tether Decorator
    @db.tether(bucket="etcd_logs", queue=True, backend="etcd")
    def etcd_tether_example():
        return {"key": "etcd_tether_key", "value": "etcd_tethered_data"}

    etcd_tether_example()
    print("Tethered write to etcd backend complete.")

    # Wait for writes and list keys
    time.sleep(3)
    print("Listing keys from etcd backend:")
    etcd_keys = db.list_keys(backend="etcd")
    print(etcd_keys)

    # Stress Test
    stress_test(db, "etcd", num_writes=20)

    # ================================
    # Shutdown
    # ================================
    print("\nShutting down the background worker...")
    db.stop()
    print("Done!")


if __name__ == "__main__":
    main()
