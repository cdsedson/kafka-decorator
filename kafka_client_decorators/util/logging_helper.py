#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Define function used on logging."""

import logging

KAFKA_DECORATOR_DEBUG = None


def set_debug_level(level):
    """Set the level of log.

    Set logging level for all loggers create by get_logger function

    Parameters
    ----------
        level: log level define in logging module
    """
    global KAFKA_DECORATOR_DEBUG
    KAFKA_DECORATOR_DEBUG = level


def get_logger(name):
    """Create and return a logger.

    Parameters
    ----------
        name: str
           Logger name

    Returns
    -------
        logging.Logger
            A standard python logger
    """
    global KAFKA_DECORATOR_DEBUG
    logger = logging.getLogger(name)
    if KAFKA_DECORATOR_DEBUG is not None:
        logger.setLevel(KAFKA_DECORATOR_DEBUG)
    return logger
