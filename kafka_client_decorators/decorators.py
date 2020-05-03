#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Define KafkaDecorator class."""

from .client import Client, ConsumerBuilder, ProducerBuilder
from .kafka import ConnectionBuilder
from .util import get_logger


class KafkaDecorator:
    """Wrap pykafka functions.

    Define the decorators and hold the comunication data
    """

    def __init__(self):
        """Create a KafkaDecorator."""
        self.logger = get_logger(__name__)
        self.logger.info("Creating decorator ")

        self.__topics_receive__ = []
        self.__topics_send__ = []
        self.__connection__ = None
        self.cls = None

    def host(self, *args, **kargs):
        """Set the conenction data.

        Create a new version of the decorated class that inherits from
        kafka_client_decorators.client.Client class

        Parameters
        ----------
           args: *args
                A list of arguments used by pykafka.client.KafkaClient
                initialization
            kargs: **kargs
                {key:value} format list used by pykafka.client.KafkaClient
                initialization

        Returns
        -------
            class
                A new class that inherits from the Client class
        """
        self.logger.info("Adding host")

        def inner(cls):
            class NewCls(Client, cls):
                def __init__(obj, *iargs, **ikargs):
                    self.logger.info(f"Creating class: {cls}")
                    Client.__init__(obj, self.__connection__,
                                    self.__topics_receive__,
                                    self.__topics_send__)
                    cls.__init__(obj, *iargs, **ikargs)
                    self.cls = obj
            return NewCls
        self.__connection__ = ConnectionBuilder(args, kargs)
        return inner

    def balanced_consumer(self, topic, *args, **kargs):
        """Create a balanced consumer.

        The created consumer will call the decorated function
        every time a message was received

        Parameters
        ----------
            topic: str
                The name of the topic that will be read
            args: *args
                A list of arguments used by
                pykafka.topic.Topic.get_balanced_consumer function
            kargs: **kargs
                {key:value} format list used by
                pykafka.topic.Topic.get_balanced_consumer function

        Returns
        -------
            function
                The same decorated function
        """
        self.logger.info(f"Adding balanced consumer, topic: {topic}")

        def kafka_client_consumer_inner(function):
            c_conf = ConsumerBuilder(topic, True, args, kargs, function)
            self.__topics_receive__.append(c_conf)
            return function
        return kafka_client_consumer_inner

    def simple_consumer(self, topic, *args, **kargs):
        """Create a simple consumer.

        The created consumer will call the decorated function
        every time a message was received

        Parameters
        ----------
            topic: str
                The name of the topic that will be read
            args: *args
                A list of arguments used by
                pykafka.topic.Topic.get_simple_consumer function
            kargs: **kargs
                {key:value} format list used by
                pykafka.topic.Topic.get_simple_consumer function

        Returns
        -------
            function
                The same decorated function
        """
        self.logger.info(f"Adding simple consumer, topic: {topic}")

        def kafka_client_consumer_inner(function):
            c_conf = ConsumerBuilder(topic, False, args, kargs, function)
            self.__topics_receive__.append(c_conf)
            return function
        return kafka_client_consumer_inner

    def producer(self, topic, *args, **kargs):
        """Create a producer.

        The created a producer and a function that used it for
        send messages to the topic.
        The definition of the function decorated will be ignored,
        just its name will be used

        Parameters
        ----------
            topic: str
                The name of the topic that will be written
            args: *args
                A list of arguments used by
                pykafka.topic.Topic.get_producer function
            kargs: **kargs
                {key:value} format list used by
                pykafka.topic.Topic.get_producer function

        Returns
        -------
            function
                A new function that send messages
                that function have the same parameters of
                pykafka.producer.Producer.produce function
        """
        self.logger.info(f"Adding producer, topic: {topic}")

        def inner_producer(function):
            p_conf = ProducerBuilder(function.__name__, topic, args, kargs)
            self.__topics_send__.append(p_conf)

            def inner(obj, *func_args, **func_kargs):
                return obj.producer(function.__name__, *func_args, **func_kargs)
            return inner
        return inner_producer
