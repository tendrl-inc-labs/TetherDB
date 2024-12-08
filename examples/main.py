from TetherDB import DB
import time

def main():
    # Path to the config file
    config_file = "config.json"

    # Initialize the DB instance
    db = DB(config_file)

    # ================================
    # Local Backend Operations
    # ================================
    print("== Local Backend Operations ==")
    # Direct Write
    db.write_message("local_key1", {"data": "local_direct_write"}, backend="local")
    print("Direct write to Local backend successful.")

    # Queued Write
    db.write_message("local_key2", {"data": "local_queued_write"}, backend="local", queue=True)
    print("Queued write to Local backend enqueued.")

    # Using Tether Decorator
    @db.tether(bucket="local_logs", wait=True, backend="local")
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

    # ================================
    # DynamoDB Backend Operations
    # ================================
    print("\n== DynamoDB Backend Operations ==")
    # Direct Write
    db.write_message("dynamo_key1", {"data": "dynamo_direct_write"}, backend="dynamodb")
    print("Direct write to DynamoDB backend successful.")

    # Queued Write
    db.write_message("dynamo_key2", {"data": "dynamo_queued_write"}, backend="dynamodb", queue=True)
    print("Queued write to DynamoDB backend enqueued.")

    # Using Tether Decorator
    @db.tether(bucket="dynamo_logs", wait=False, backend="dynamodb")
    def dynamo_tether_example():
        return {"key": "dynamo_tether_key", "value": "dynamo_tethered_data"}

    dynamo_tether_example()
    print("Tethered write to DynamoDB backend enqueued.")

    # Give time for queued writes to process
    time.sleep(3)

    # Retrieve keys from DynamoDB Backend
    print("Listing keys from DynamoDB backend:")
    dynamo_keys = db.list_keys(backend="dynamodb")
    print(dynamo_keys)

    # ================================
    # etcd Backend Operations
    # ================================
    print("\n== etcd Backend Operations ==")
    # Direct Write
    db.write_message("etcd_key1", "etcd_direct_write", backend="etcd")
    print("Direct write to etcd backend successful.")

    # Queued Write
    db.write_message("etcd_key2", "etcd_queued_write", backend="etcd", queue=True)
    print("Queued write to etcd backend enqueued.")

    # Using Tether Decorator
    @db.tether(bucket="etcd_logs", wait=True, backend="etcd")
    def etcd_tether_example():
        return {"key": "etcd_tether_key", "value": "etcd_tethered_data"}

    etcd_tether_example()
    print("Tethered write to etcd backend complete.")

    # Give time for queued writes to process
    time.sleep(3)

    # Retrieve keys from etcd Backend
    print("Listing keys from etcd backend:")
    etcd_keys = db.list_keys(backend="etcd")
    print(etcd_keys)

    # ================================
    # Pagination Example (Local)
    # ================================
    print("\n== Pagination Example (Local Backend) ==")
    print("Writing multiple keys for pagination demo...")
    for i in range(20):
        db.write_message(f"paginated_key_{i}", f"value_{i}", backend="local", queue=True)

    # Wait for queued writes to process
    time.sleep(3)

    # Paginate keys in Local backend
    print("Paginating through keys:")
    page_size = 5
    next_marker = None
    while True:
        result = db.list_keys(page_size=page_size, start_after=next_marker, backend="local")
        keys = result["keys"]
        next_marker = result["next_marker"]

        print(f"Keys: {keys}")
        if not next_marker:
            break

    # ================================
    # Shutdown
    # ================================
    print("\nShutting down the background worker...")
    db.worker.stop()
    print("Done!")


if __name__ == "__main__":
    main()