#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Define ConsumerFactory class."""


class ConsumerFactory:
    """A class capable of create a consumer connection."""

    def __init__(self, connection, topic, balanced, args, kargs):
        """Create a ConsumerFactory.

        A object capable of create a consumer connection

        Parameters
        ----------
            connection: ConnectionBuilder
                A object that holds the connection information
            topic: str
                Name of topic that messages will be read from
            balanced: bool
                True for a balanced consumer
                False for a simple consumer
            *args
                A list of arguments used by the create consumer function
            **kargs
                {key:value} format list used by the create consumer function
        """
        self.__args__ = args
        self.__kargs__ = kargs
        self.__topic__ = topic
        self.__conn__ = connection
        self.__balanced__ = balanced

    def create(self):
        """Create a consumer.

        for a balanced consumer, it will create the consumer with
        pykafka.topic.Topic.get_balanced_consumer function
        for a simple consumer, it will create the consumer with
        pykafka.topic.Topic.get_simple_consumer function

        Returns
        -------
            pykafka.balancedconsumer.BalancedConsumer
            or pykafka.simpleconsumer.SimpleConsumer
                A pykafka representation of a consumer
        """
        cons = None
        topic = self.__conn__.create_topic(self.__topic__)

        if self.__balanced__ is True:
            cons = topic.get_balanced_consumer(
                *self.__args__, **self.__kargs__)
        else:
            cons = topic.get_simple_consumer(*self.__args__, **self.__kargs__)
        return cons

    def __str__(self):
        """Return a string representation.

        Returns
        -------
            str
               returns a string representation of itself
        """
        str_type = ''
        if self.__balanced__ is True:
            str_type = 'Balance'
        else:
            str_type = 'Simple'
        return f"Consumer {str_type}, topic: {self.__topic__}"
