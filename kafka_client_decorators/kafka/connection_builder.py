#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Define ConnectionBuilder class."""

from pykafka import KafkaClient

from .consumer_factory import ConsumerFactory
from .producer_factory import ProducerFactory


class ConnectionBuilder:
    """Create a conection with kafka.

    Hold the connection parameters and create a connection
    using it.
    """

    def __init__(self, args, kargs):
        """Create a ConnectionBuilder object.

        Parameters
        ----------
            args: *args
                A list of arguments used by pykafka.client.KafkaClient
                initialization
            kargs: **kargs
                {key:value} format list used by pykafka.client.KafkaClient
                initialization
        """
        self.__args__ = args
        self.__kargs__ = kargs

    def create_topic(self, topic):
        """Create a topic.

        Parameters
        ----------
            topic: str
                Name of the topic to be used

        Returns
        -------
            pykafka.topic.Topic
                pykafka representation of a topic

        """
        kafka_client = KafkaClient(*self.__args__, **self.__kargs__)
        return kafka_client.topics[topic]

    def get_consumer(self, topic, balanced, args, kargs):
        """Create a ConsumerFactory.

        Parameters
        ----------
            topic: str
                Name of the topic
            balanced: bool
                True for a balanced consumer
                False to a simple consumer
            args: *args
                A list of arguments passed to __init__ method
                of ConsumerFactory
            kargs: **kargs
                {key:value} format list to __init__ method
                of ConsumerFactory

        Returns
        -------
            ConsumerFactory
                A object capable of create a consumer connection
        """
        return ConsumerFactory(self, topic, balanced, args, kargs)

    def get_producer(self, topic, args, kargs):
        """Create a ProducerFactory.

        Parameters
        ----------
            topic: str
                Name of the topic
            args: *args
                A list of arguments passed to __init__ method
                of ProducerFactory
            kargs: **kargs
                {key:value} format list to __init__ method
                of ProducerFactory

        Returns
        -------
            ProducerFactory
                A object capable of create a producer connection
        """
        return ProducerFactory(self, topic, args, kargs)
