#!/usr/bin/python3
# -*- coding: utf-8 -*-

from . import ConsumerJob
from . import ConsumerFactory


class ConsumerBuilder:
    SIMPLE = ConsumerFactory.SIMPLE
    BALANCED = ConsumerFactory.BALANCED

    def __init__(self, kind, topic, args, kargs, function):
        self.__args__ = args
        self.__kargs__ = kargs
        self.__function__ = function
        self.__topic__ = topic
        self.__kind__ = kind

    def create(self, parent):
        conn = parent.getConnection()
        prod = ConsumerFactory(conn, self.__topic__, self.__kind__,
                               self.__args__, self.__kargs__)
        return ConsumerJob(parent, self.__function__, prod)

    def __str__(self):
        return f"topic: {self.__topic__}"
