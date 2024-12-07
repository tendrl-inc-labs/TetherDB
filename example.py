from TetherDB import DB
import time

# Initialize the database with the configuration file
db = DB("config.json")

def main():
    print("=== Testing TetherDB Library ===\n")

    # 1. Direct Writes
    print("1. Direct Writes:")
    db.write_message("user:alice", {"name": "Alice", "role": "admin"}, bucket="users", backend="local")
    db.write_message("config:test1", "Test Config Value", bucket="configs", backend="etcd")
    print(" - Direct writes complete.\n")

    # 2. Queued Writes
    print("2. Queued Writes:")
    db.start()  # Start the background worker for queued writes

    db.write_queue_message("user:bob", {"name": "Bob", "role": "editor"}, bucket="users", backend="local")
    db.write_queue_message("config:test2", {"config": "Queued Config"}, bucket="configs", backend="etcd")
    print(" - Messages queued for background writing.")
    print(" - Allowing time for the background worker to process...\n")
    time.sleep(3)

    # 3. Tether Decorator: Custom Keys
    print("3. Tether Decorator: Custom Keys")
    @db.tether(bucket="users", backend="local", wait=True)
    def get_custom_user():
        return {"key": "user:charlie", "value": {"name": "Charlie", "role": "viewer"}}

    get_custom_user()
    print(" - Tethered write with custom key complete.\n")

    # 4. Tether Decorator: Automatic UUID
    print("4. Tether Decorator: Automatic UUID")
    @db.tether(bucket="configs", backend="etcd", wait=False)
    def generate_auto_config():
        return {"value": {"config": "Generated Config", "status": "active"}}

    generate_auto_config()
    print(" - Tethered write with automatic UUID queued.\n")

    # 5. Invalid Return Value Handling
    print("5. Invalid Return Value Handling")
    @db.tether(bucket="errors", backend="local", wait=True)
    def invalid_return():
        return {"key": "invalid:data", "value": [1, 2, 3]}  # Invalid type (list)

    invalid_return()
    print(" - Invalid return value handled gracefully.\n")

    # 6. Edge Case: Missing Key and Value
    print("6. Edge Case: Missing Key and Value")
    @db.tether(bucket="errors", backend="local")
    def missing_value():
        return {}  # Missing 'value' key

    missing_value()
    print(" - Missing 'value' key handled gracefully.\n")

    # 7. Batch Writes Test
    print("7. Batch Writes Test")
    for i in range(1, 6):
        db.write_queue_message(f"user:auto{i}", {"name": f"User_{i}", "role": "batch-test"}, bucket="users", backend="local")
    print(" - Queued 5 messages for batch processing.\n")

    # Allow background worker to process queued writes
    time.sleep(3)

    # Stop the worker
    print("Stopping the background worker and cleaning up...")
    db.stop()
    print("=== Testing Complete ===")


if __name__ == "__main__":
    main()