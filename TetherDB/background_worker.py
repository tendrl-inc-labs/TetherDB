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
        """
        Initialize the BackgroundWorker.

        :param queue: Queue containing messages to process.
        :param process_batch: Function to process a batch of messages.
        :param logger: Logger instance for debug and info logs.
        :param lock: Thread lock for synchronized writes.
        """
        self.queue = queue
        self.process_batch = process_batch
        self.logger = logger
        self.lock = lock
        self.thread = None
        self.is_running = False  # Proper flag for thread management
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
            self.is_running = False
            self.queue.put(None)  # Signal the worker thread to terminate
            if self.thread:
                self.thread.join(timeout=5)  # Avoid indefinite hanging
            self.logger.debug("Background worker stopped.")

    def _worker_loop(self):
        """Process queued messages in batches."""
        batch = []
        while self.is_running:
            try:
                item = self.queue.get(timeout=self.batch_timeout)
                if item is None:  # Stop signal received
                    break
                batch.append(item)
                self.queue.task_done()

                if len(batch) >= self.batch_size:
                    self._process_and_clear_batch(batch)

            except Empty:
                # Timeout reached; process any pending batch
                if batch:
                    self._process_and_clear_batch(batch)

        # Final cleanup: process remaining messages in the batch
        if batch:
            self._process_and_clear_batch(batch)
        self.logger.debug("Worker loop exited cleanly.")

    def _process_and_clear_batch(self, batch):
        """Process and clear the current batch."""
        with self.lock:
            self.process_batch(batch)
        batch.clear()
