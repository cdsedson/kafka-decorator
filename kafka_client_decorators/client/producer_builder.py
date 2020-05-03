#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Define ProducerBuilder class."""

from .producer import Producer


class ProducerBuilder:
    """Work as a factory to create a topic producer."""

    def __init__(self, name, topic, args, kargs):
        """Create a ProducerBuilder object.

        Parameters
        ----------
            name: str
                The name of a function, it is just used as a hash key
                to map a function called and the right topic destination
            topic: str
                The name of topic wich the producer will connect
            *args
                List of arguments to be passed to producer
            **kargs
                {key:value} format list to be passed to producer
        """
        self.name = name
        self.__args__ = args
        self.__kargs__ = kargs
        self.__topic__ = topic

    def create(self, parent):
        """Create a Producer object.

        Parameters
        ----------
        parent: Client
            Just used to obtain connection parameters

        Returns
        -------
            Producer
                A new Producer object configured with
                the parameters given to __init__
        """
        conn = parent.get_connection()
        prod = conn.get_producer(self.__topic__,
                                 self.__args__, self.__kargs__)
        return Producer(prod)

    def __str__(self):
        """Return a string representation.

        Returns
        -------
            str
               returns a string representation of itself
        """
        return f"funcion: {self.name}, topic: {self.__topic__}"
