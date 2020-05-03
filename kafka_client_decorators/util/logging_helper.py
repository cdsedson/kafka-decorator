#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Define function used on logging."""

import logging

__KAFKA_DECORATOR_DEBUG__ = None


def set_debug_level(level):
    """Set the level of log.

    Set logging level for all loggers create by get_logger function

    Parameters
    ----------
        level: log level define in logging module
    """
    global __KAFKA_DECORATOR_DEBUG__
    __KAFKA_DECORATOR_DEBUG__ = level


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
    logger = logging.getLogger(name)
    if __KAFKA_DECORATOR_DEBUG__ is not None:
        logger.setLevel(__KAFKA_DECORATOR_DEBUG__)
    return logger
