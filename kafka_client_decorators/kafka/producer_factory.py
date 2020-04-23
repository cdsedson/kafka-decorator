#!/usr/bin/python3
# -*- coding: utf-8 -*-


class ProducerFactory:
    def __init__(self, conn, topic, args, kargs):
        self.__args__ = args
        self.__kargs__ = kargs
        self.__topic__ = topic
        self.__conn__ = conn

    def create(self):
        kafka_client = self.__conn__.create()
        t = kafka_client.topics[self.__topic__]
        return t.get_producer(*self.__args__, **self.__kargs__)

    def __str__(self):
        return f"topic: {self.__topic__}"
