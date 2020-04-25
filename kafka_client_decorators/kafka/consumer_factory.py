#!/usr/bin/python3
# -*- coding: utf-8 -*-


class ConsumerFactory:
    def __init__(self, conn, topic, balanced, args, kargs):
        self.__args__ = args
        self.__kargs__ = kargs
        self.__topic__ = topic
        self.__conn__ = conn
        self.__balanced__ = balanced

    def create(self):
        cons = None
        topic = self.__conn__.create_topic(self.__topic__)

        if self.__balanced__ is True:
            cons = topic.get_balanced_consumer(
                *self.__args__, **self.__kargs__)
        else:
            cons = topic.get_simple_consumer(*self.__args__, **self.__kargs__)
        return cons

    def __str__(self):
        str_type = ''
        if self.__balanced__ is True:
            str_type = 'Balance'
        else:
            str_type = 'Simple'
        return f"Consumer {str_type}, topic: {self.__topic__}"
