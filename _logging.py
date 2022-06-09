import logging
import logging.handlers
import sys
from typing import Dict

from _globals import *


class LogCounterFilter(logging.Filter):
    def __init__(self) -> None:
        super().__init__()
        self.counters: Dict[str, int] = {}

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno > logging.INFO:
            self.counters[record.levelname] = self.counters.get(record.levelname, 0) + 1
        return True


class LevelsFilter(logging.Filter):
    def __init__(self, levels: tuple) -> None:
        super().__init__()
        self.levels = levels

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno in self.levels


try:
    LOG_FILE.parent.mkdir(exist_ok=True)
except Exception as err:
    sys.exit(err)

log = logging.getLogger()
log.setLevel(level=logging.INFO)

# creates a stdout handler
log_stdout_handler = logging.StreamHandler(sys.stdout)
log_stdout_handler.setLevel(logging.INFO)
log.addHandler(log_stdout_handler)
log_stdout_filter = LevelsFilter(levels=(logging.INFO,))
log_stdout_handler.addFilter(log_stdout_filter)

# creates a stderr handler
log_stderr_handler = logging.StreamHandler(sys.stderr)
log_stderr_handler.setLevel(level=logging.CRITICAL)
stderr_formatter = logging.Formatter(fmt="[{levelname}]: {message}", style='{')
log_stderr_handler.setFormatter(stderr_formatter)
log.addHandler(log_stderr_handler)

# creates a file handler
log_file_handler = logging.handlers.RotatingFileHandler(filename=LOG_FILE, maxBytes=(1024 * 1024), backupCount=9)
record_format = "<{asctime}> [{levelname: ^8}] ({module}::{funcName}):  {message}"
file_formatter = logging.Formatter(fmt=record_format, datefmt='%Y-%m-%d %H:%M:%S %Z', style='{')
log_file_handler.setFormatter(file_formatter)
log_counter = LogCounterFilter()
log_file_handler.addFilter(log_counter)
# rotates the log file
log_file_handler.doRollover()
log.addHandler(log_file_handler)
