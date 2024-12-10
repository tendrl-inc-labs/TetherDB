import threading
from queue import Empty


class BackgroundWorker:
    """Background worker to process queued writes in batches."""

    __slots__ = [
        "queue",
        "process_batch",
        "logger",
        "lock",
        "thread",
        "is_running",
        "batch_size",
        "batch_timeout",
    ]

    def __init__(self, queue, process_batch, logger, lock):
        self.queue = queue
        self.process_batch = process_batch
        self.logger = logger
        self.lock = lock
        self.thread = None
        self.is_running = False
        self.batch_size = 10
        self.batch_timeout = 2.0

    def start(self, batch_size=10, batch_timeout=2.0):
        """Start the background worker thread."""
        if not self.is_running:
            self.is_running = True
            self.batch_size = batch_size
            self.batch_timeout = batch_timeout
            self.thread = threading.Thread(target=self._worker_loop, daemon=True)
            self.thread.start()
            self.logger.debug("Background worker started.")

    def stop(self):
        """Stop the background worker thread gracefully."""
        if self.is_running:
            self.logger.debug("Stopping background worker...")
            self.is_running = False
            self.queue.put(None)  # Send stop signal
            self.thread.join(timeout=5)
            self.logger.debug("Background worker stopped.")

    def _worker_loop(self):
        """Main loop to process messages from the queue in batches."""
        batch = []
        while self.is_running:
            try:
                item = self.queue.get(timeout=self.batch_timeout)
                if item is None:  # Stop signal
                    self.logger.debug("Stop signal received. Exiting worker loop.")
                    break

                batch.append(item)
                self.queue.task_done()

                if len(batch) >= self.batch_size:
                    self._process_and_clear_batch(batch)

            except Empty:
                # Batch timeout reached; process partial batch
                if batch:
                    self._process_and_clear_batch(batch)

        # Final cleanup: Process any remaining batch
        if batch:
            self._process_and_clear_batch(batch)
        self.logger.debug("Worker loop exited cleanly.")

    def _process_and_clear_batch(self, batch):
        """Process and clear the current batch."""
        try:
            self.process_batch(batch)  # Call the batch processor atomically
            self.logger.debug(f"Batch processed: {len(batch)} items")
        except Exception as e:
            self.logger.error(f"Error processing batch: {e}")
        finally:
            batch.clear()  # Ensure the batch is cleared regardless of errors
