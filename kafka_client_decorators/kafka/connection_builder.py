#!/usr/bin/python3
# -*- coding: utf-8 -*-

from pykafka import KafkaClient

class ConnectionBuilder:
    def __init__(self, args, kargs):
        self.__args__ = args
        self.__kargs__ = kargs

    def create(self):
        return KafkaClient(*self.__args__, **self.__kargs__)
