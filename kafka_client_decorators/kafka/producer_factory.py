#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Define ProducerFactory class."""


class ProducerFactory:
    """A class capable of create a producer connection."""

    def __init__(self, connection, topic, args, kargs):
        """Create a ProducerFactory object.

        Parameters
        ----------
            connection: ConnectionBuilder
                A object that holds the connection information
            topic: str
                Name of topic that messages will be read from
            *args
                A list of arguments used by the create producer function
            **kargs
                {key:value} format list used by the create producer function
        """
        self.__args__ = args
        self.__kargs__ = kargs
        self.__topic__ = topic
        self.__conn__ = connection

    def create(self):
        """Create a producer.

        Calls pykafka.topic.Topic.get_producer to create the producer
        Returns
        -------
            pykafka.producer.Producer
        """
        topic = self.__conn__.create_topic(self.__topic__)
        return topic.get_producer(*self.__args__, **self.__kargs__)

    def __str__(self):
        """Return a string representation.

        Returns
        -------
            str
               returns a string representation of itself
        """
        return f"topic: {self.__topic__}"
