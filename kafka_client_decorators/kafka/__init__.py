#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Manage the integration with kafka.

Hold the objects used to make a integration interface with kafka
This inferface is build over pykafka api
"""

from .connection_builder import ConnectionBuilder

__all__ = ['ConnectionBuilder']
