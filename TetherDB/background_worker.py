import threading
from queue import Empty


class BackgroundWorker:
    """
    Background worker to process queued writes in batches.
    """

    __slots__ = [
        "queue",
        "backends",
        "logger",
        "lock",
        "thread",
        "is_running",
        "batch_size",
        "batch_interval",
    ]

    def __init__(self, queue, backends, logger):
        """
        Initialize the BackgroundWorker.

        :param queue: Queue containing tasks to process.
        :param backends: Backends instance for performing writes.
        :param logger: Logger instance for debug and error logging.
        """
        self.queue = queue
        self.backends = backends  # Reference to the backends
        self.logger = logger
        self.lock = threading.Lock()  # Local lock for atomic batch processing
        self.thread = None
        self.is_running = False
        self.batch_size = 10
        self.batch_interval = 2.0

    def start(self, batch_size=10, batch_interval=2.0):
        """
        Start the background worker thread.

        :param batch_size: Maximum number of items to process in one batch.
        :param batch_interval: Maximum wait time (seconds) for the next batch.
        """
        if self.is_running:
            self.logger.debug("Background worker is already running.")
            return

        self.is_running = True
        self.batch_size = batch_size
        self.batch_interval = batch_interval
        self.thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.thread.start()
        self.logger.debug("Background worker started.")

    def stop(self):
        """
        Stop the background worker thread gracefully.
        """
        if not self.is_running:
            self.logger.debug("Background worker is not running.")
            return

        self.logger.debug("Stopping background worker...")
        try:
            self.is_running = False
            self.queue.put(None)  # Send stop signal
            self.thread.join(timeout=5)
        except Exception as e:
            self.logger.error(f"Error stopping worker thread: {e}")
        finally:
            self.thread = None
            self.logger.debug("Background worker stopped.")

    def _worker_loop(self):
        """
        Main loop to process messages from the queue in batches.
        """
        batch = []
        while self.is_running:
            try:
                # Wait for an item in the queue
                item = self.queue.get(timeout=self.batch_interval)
                if item is None:  # Stop signal
                    self.logger.debug("Stop signal received. Exiting worker loop.")
                    break

                batch.append(item)
                self.queue.task_done()

                # Process batch if size limit is reached
                if len(batch) >= self.batch_size:
                    self._process_batch(batch)

            except Empty:
                # Process any remaining items after the batch interval
                if batch:
                    self._process_batch(batch)

        # Final cleanup: Process any remaining batch
        if batch:
            self._process_batch(batch)
        self.logger.debug("Worker loop exited cleanly.")

    def _process_batch(self, batch):
        """
        Process and clear the current batch of tasks.

        :param batch: List of queued (key, value, backend) tasks.
        """
        try:
            with self.lock:  # Lock to ensure thread-safe writes
                for key, value, backend in batch:
                    self.backends.write(key, value, backend)
            self.logger.debug(f"Batch processed: {len(batch)} items.")
        except Exception as e:
            self.logger.error(f"Error processing batch: {e}")
        finally:
            batch.clear()
