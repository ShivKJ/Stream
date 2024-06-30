"""
author: Shiv
email: shivkj001@gmail.com
"""

from datetime import date
from logging import DEBUG, Formatter, INFO, StreamHandler, getLogger
from logging.handlers import RotatingFileHandler
from sys import stdout

try:
    from streamAPI.utility import LOG_FILE
except ImportError:
    from warnings import warn

    LOG_FILE = str(date.today()) + '.log'
    warn(f'Log File is not set in utility.config.py . Using {LOG_FILE} as log file')

LOG_FORMAT = '%(asctime)s %(levelname)s [%(filename)s] [%(lineno)d] %(message)s'

DEFAULT_LOG_LEVEL = INFO
RF_HANDLER_LEVEL = INFO
STREAM_HANDLER_LEVEL = DEBUG
LOG_FILE_SIZE = 1024 * 1024 * 8
BACKUP_COUNT = 5

LOGGER_NAME = 'BASICS'


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
    sh.setLevel(STREAM_HANDLER_LEVEL)
    logger.addHandler(sh)

    return logger


initialize_logger(LOGGER_NAME)
