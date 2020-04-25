#!/usr/bin/python3
# -*- coding: utf-8 -*-


class ProducerFactory:
    def __init__(self, conn, topic, args, kargs):
        self.__args__ = args
        self.__kargs__ = kargs
        self.__topic__ = topic
        self.__conn__ = conn

    def create(self):
        topic = self.__conn__.create_topic(self.__topic__)
        return topic.get_producer(*self.__args__, **self.__kargs__)

    def __str__(self):
        return f"topic: {self.__topic__}"
