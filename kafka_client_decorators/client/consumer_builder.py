#!/usr/bin/python3
# -*- coding: utf-8 -*-

from .consumer_job import ConsumerJob


class ConsumerBuilder:

    def __init__(self, topic, balanced, args, kargs, function):
        self.__args__ = args
        self.__kargs__ = kargs
        self.__function__ = function
        self.__topic__ = topic
        self.__balanced__ = balanced

    def create(self, parent):
        conn = parent.getConnection()
        prod = conn.get_consumer(self.__topic__, self.__balanced__,
                                 self.__args__, self.__kargs__)
        return ConsumerJob(parent, self.__function__, prod)

    def __str__(self):
        return f"topic: {self.__topic__}"
