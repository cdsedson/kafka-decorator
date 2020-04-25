#!/usr/bin/python3
# -*- coding: utf-8 -*-

import logging

KAFKA_DECORATOR_DEBUG = None


def set_debug_level(val):
    global KAFKA_DECORATOR_DEBUG
    KAFKA_DECORATOR_DEBUG = val


def get_logger(name):
    global KAFKA_DECORATOR_DEBUG
    logger = logging.getLogger(name)
    if KAFKA_DECORATOR_DEBUG is not None:
        logger.setLevel(KAFKA_DECORATOR_DEBUG)
    return logger
