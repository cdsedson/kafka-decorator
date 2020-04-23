#!/usr/bin/python3
# -*- coding: utf-8 -*-


class ConsumerFactory:
    SIMPLE = 1
    BALANCED = 2

    def __init__(self, conn, topic, kind, args, kargs):
        self.__args__ = args
        self.__kargs__ = kargs
        self.__topic__ = topic
        self.__conn__ = conn
        self.__kind__ = kind

    def create(self):
        cons = None
        kafka_client = self.__conn__.create()
        t = kafka_client.topics[self.__topic__]
        if self.__kind__ == ConsumerFactory.BALANCED:
            cons = t.get_balanced_consumer(*self.__args__, **self.__kargs__)
        else:
            cons = t.get_simple_consumer(*self.__args__, **self.__kargs__)
        return cons

    def __str__(self):
        str_type =''
        if self.__kind__ == ConsumerFactory.BALANCED:
            str_type = 'Balance'
        else:
            str_type = 'Simple'
        return f"Consumer {str_type}, topic: {self.__topic__}"

