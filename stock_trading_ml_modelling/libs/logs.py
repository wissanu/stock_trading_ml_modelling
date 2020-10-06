import logging
import logging.config
from pathlib import Path

from stock_trading_ml_modelling.logger_config import LOGGER_CONFIG

def _set_logger(filename, logger_conf=LOGGER_CONFIG):
    logger_conf["handlers"]["file"]["filename"] = str(Path(__file__).parent.parent.parent / "logs" / f"{filename}.log")
    logging.config.dictConfig(logger_conf)
    return logging.getLogger(str(filename))

def default_logger():
    logger_path = Path(__file__).parent.parent.parent / "logs" / Path(__file__) \
        .stem.upper()
    return _set_logger(logger_path, LOGGER_CONFIG)

class NoLog:
    def info(self, msg):
        print(f"INFO: {msg}")
    def warning(self, msg):
        print(f"WARNING: {msg}")
    def error(self, msg):
        print(f"ERROR: {msg}")

class Logger:
    def __init__(self):
        self.log = NoLog()
        self.log_name = None

    def set_logger(self, filename):
        #Write logs to one file, not lots.
        # Once a log file is set, don't change it
        if not self.log_name:
            log.info(f"Log name set to {filename}")
            self.log = _set_logger(filename, logger_conf=LOGGER_CONFIG)
            self.log_name = filename
    
    def info(self, msg):
        self.log.info(msg)

    def warning(self, msg):
        self.log.warning(msg)

    def error(self, msg):
        self.log.error(msg)

log = Logger()