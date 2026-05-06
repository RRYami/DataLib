"""Root logger configuration for the application."""

import atexit
import json
import logging.config
import pathlib
from typing import override

root_path = pathlib.Path(__file__).parent.parent
log_dir = root_path / "logs"
log_dir.mkdir(exist_ok=True)


def setup_logging():
    config_path = root_path / "log_config" / "config.json"
    with open(config_path, "r", encoding="utf-8") as config_file:
        config = json.load(config_file)
    logging.config.dictConfig(config)
    queue_handler = logging.getHandlerByName("queue_handler")
    if queue_handler is not None:
        queue_handler.listener.start()  # type: ignore
        atexit.register(queue_handler.listener.stop)  # type: ignore


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name.

    This logger will inherit from the root logger configuration.

    Args:
        name: The name of the logger (typically __name__ of the module)

    Returns:
        A configured logger instance
    """
    return logging.getLogger(name)


class MyJsonFormatter(logging.Formatter):
    """
    Custom JSON formatter for logging.
    """

    def __init__(self, fmt_keys: dict[str, str] | None = None, **kwargs):
        super().__init__(**kwargs)
        self.fmt_keys = fmt_keys or {}

    @override
    def format(self, record: logging.LogRecord) -> str:
        log_record = {}

        # Use fmt_keys mapping if provided, otherwise use defaults
        if self.fmt_keys:
            for key, attr in self.fmt_keys.items():
                if attr == "asctime":
                    log_record[key] = self.formatTime(record, self.datefmt)
                else:
                    log_record[key] = getattr(record, attr, None)
        else:
            # Default fields if no fmt_keys provided
            log_record = {
                "timestamp": self.formatTime(record, self.datefmt),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            }

        # Always include message properly formatted
        if "message" in log_record:
            log_record["message"] = record.getMessage()

        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_record)
