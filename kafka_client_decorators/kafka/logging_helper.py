#!/usr/bin/python
# -*- coding: <encoding name> -*-

import logging

kafka_decorator_debug = None

def set_debug_level( val ):
    global kafka_decorator_debug
    kafka_decorator_debug = val

def get_logger( name ):
    global kafka_decorator_debug
    logger = logging.getLogger(name)
    if kafka_decorator_debug is not None:
        logger.setLevel(kafka_decorator_debug)
    return logger 


