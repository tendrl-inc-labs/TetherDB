from queue import Empty
import threading

class BackgroundWorker:
    """
    Background worker for processing queued writes.
    """
    __slots__ = ["queue", "process_function", "logger", "lock", "_running", "_worker_thread"]

    def __init__(self, queue, process_function, logger, lock):
        self.queue = queue
        self.process_function = process_function
        self.logger = logger
        self.lock = lock
        self._running = False
        self._worker_thread = None

    def start(self, batch_size, batch_timeout):
        self._running = True
        self.logger.debug("Starting background worker...")
        self._worker_thread = threading.Thread(
            target=self._worker, args=(batch_size, batch_timeout), daemon=True
        )
        self._worker_thread.start()

    def stop(self):
        self._running = False
        self.queue.put(None)
        self._worker_thread.join()
        self.logger.debug("Background worker stopped.")

    def _worker(self, batch_size, batch_timeout):
        batch = []
        while self._running:
            try:
                item = self.queue.get(timeout=batch_timeout)
                if item is None:
                    break
                batch.append(item)
                self.queue.task_done()
                if len(batch) >= batch_size:
                    with self.lock:
                        self.process_function(batch)
                    batch = []
            except Empty:
                if batch:
                    with self.lock:
                        self.process_function(batch)
                    batch = []
        if batch:
            with self.lock:
                self.process_function(batch)
        self.logger.debug("Background worker shutting down.")