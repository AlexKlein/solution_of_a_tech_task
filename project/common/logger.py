"""
Main point of logging of the process.
"""
import logging as pylogging
from logging import handlers

import settings as config
from .singleton import Singleton


LOG_FILE = config.LOG_FILE


class Logging(object):
    __metaclass__ = Singleton

    def __init__(
            self,
            verbose=False,
            level=pylogging.INFO,
            format='%(asctime)s %(levelname).ls %(processName)s %(name)s:%(lineno)d %(message)s',
            **kwargs
    ):
        pylogging.basicConfig(
            format=format,
            level=pylogging.DEBUG if verbose else level,
            handlers=[handlers.TimedRotatingFileHandler(LOG_FILE, when='D')])

    @property
    def get_logger(self):
        return pylogging.getLogger

    getLogger = get_logger


logging = Logging()
