#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Define ConsumerBuilder class."""

from .consumer_job import ConsumerJob


class ConsumerBuilder:
    """Work as a factory to create a topic consumer."""

    def __init__(self, topic, balanced, args, kargs, function):
        """Create a ConsumerBuilder object.

        Parameters
        ----------
            topic: str
                The name of topic wich the consumer will connect
            balanced: bool
                True to create a balanced consumer
                or False to a simple consumer
            args: *args
                List of arguments to be passed to consumer
            kargs: **kargs
                {key:value} format list to be passed to consumer
            function: f(message: msg) -> any
                a function to be called by consumer

        """
        self.__args__ = args
        self.__kargs__ = kargs
        self.__function__ = function
        self.__topic__ = topic
        self.__balanced__ = balanced

    def create(self, parent):
        """Create a ConumerJob object.

        Parameters
        ----------
        parent: Client
            An object which function will be called from
            Used to get the connection parameters

        Returns
        -------
            ConsumerJob
                A new ConsumerJob object configured with
                the parameters given to __ini__
        """
        conn = parent.get_connection()
        prod = conn.get_consumer(self.__topic__, self.__balanced__,
                                 self.__args__, self.__kargs__)
        return ConsumerJob(parent, self.__function__, prod)

    def __str__(self):
        """Return a string representation.

        Returns
        -------
            str
               returns a string representation of itself
        """
        return f"topic: {self.__topic__}"
