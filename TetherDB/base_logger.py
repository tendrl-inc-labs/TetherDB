import logging

def initialize_logger(config: dict):
    """
    Initializes a logger based on the logging level from the config.

    :param config: Dictionary with logging configuration.
    :return: Configured logger instance.
    """
    log_level = config.get("logging", "info").lower()
    log_levels = {"debug": logging.DEBUG, "info": logging.INFO, "none": logging.NOTSET}
    selected_level = log_levels.get(log_level, logging.INFO)

    logging.basicConfig(level=selected_level, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)

    if selected_level == logging.NOTSET:
        logging.disable(logging.CRITICAL)

    logger.debug("Logger initialized with level: %s", log_level)
    return logger