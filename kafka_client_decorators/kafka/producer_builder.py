#!/usr/bin/python3
# -*- coding: utf-8 -*-

from . import Producer
from . import ProducerFactory


class ProducerBuilder:
    def __init__(self, name, topic, args, kargs):
        self.name = name
        self.__args__ = args
        self.__kargs__ = kargs
        self.__topic__ = topic

    def create(self, parent):
        conn = parent.getConnection()
        prod = ProducerFactory(conn, self.__topic__,
                               self.__args__, self.__kargs__)
        return Producer(prod)

    def __str__(self):
        return f"funcion: {self.name}, topic: {self.__topic__}"
