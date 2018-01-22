from logging import getLogger, Formatter, StreamHandler
from logging.handlers import RotatingFileHandler
from sys import stdout

from configuration.config import (LOG_FORMAT, LOG_FILE)
from configuration.constants import (BACKUP_COUNT, DEFAULT_LOG_LEVEL, LOG_FILE_SIZE, RF_HANDLER_LEVEL,
                                     STREAM_HANDLER_LEVLE)


def initialize_logger(logger_name):
    logger = getLogger(logger_name)
    logger.setLevel(DEFAULT_LOG_LEVEL)
    fmt = Formatter(LOG_FORMAT)

    # ---------------Adding File Handler--------------------
    fh = RotatingFileHandler(LOG_FILE,
                             maxBytes=LOG_FILE_SIZE,
                             backupCount=BACKUP_COUNT)
    fh.setLevel(RF_HANDLER_LEVEL)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # -----------------Adding Stream Handler-----------------
    sh = StreamHandler(stdout)
    sh.setFormatter(fmt)
    sh.setLevel(STREAM_HANDLER_LEVLE)
    logger.addHandler(sh)

    return logger


logger = initialize_logger(__name__)
