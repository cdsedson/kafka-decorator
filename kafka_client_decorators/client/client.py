#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Define Client class."""

from threading import Thread
from time import sleep

from ..util import get_logger


class Client(Thread):
    """Manage all connections.

    Keep and manage all consumers and producers. Making sure
    they are health and running until the application stop
    """

    def __init__(self, connection, list_topics_receive, list_topics_send):
        """Create a Client object.

        Parameters
        ----------
            connection: ConnectionBuilder
                A object that holds the connection parameters
            list_topics_receive: [ConsumerBuilder]
                A list of consumer configurations, with all information
                needed to create the consumers
            list_topics_send: [ProducerBuilder]
                A list of producer configurations, with all information
                needed to create the producers
        """
        self.logger = get_logger(__name__)
        self.logger.info("Creating cliet, listen topics: "
                         f"{[str(t) for t in list_topics_receive]} "
                         f"send topics: {[str(t) for t in list_topics_send]}")

        self.__started__ = True
        self.__conumers_failed__ = False
        self.__list_topics_receive__ = list_topics_receive
        self.__connection_args__ = connection
        self.__list_topics_send__ = {
            p.name: (p, None) for p in list_topics_send}
        Thread.__init__(self)

    def get_connection(self):
        """Return the connection data.

        Returns
        -------
            ConnectionBuilder
                An object capable of creating a connection
        """
        return self.__connection_args__

    def __create_consumers__(self):
        """Create the consumers.

        Returns
        -------
            [ConsumerJob]
                A consumerJob list, all ready to use but not
                started yet
        """
        self.logger.info("Creating consumers")
        consumers = []

        for builder in self.__list_topics_receive__:
            job = builder.create(self)
            consumers.append(job)
        self.__conumers_failed__ = False
        return consumers

    def __start_consumers__(self, consumers):
        """Start all consumers."""
        self.logger.info("Staring consumers")
        for consumer in consumers:
            consumer.start()

    def __stop_consumer__(self, consumers):
        """Stop all cnsumers."""
        self.logger.info("Stopping consumers")
        for consumer in consumers:
            if consumer is not None:
                consumer.stop()

    def __wait_consumer__(self, consumers):
        """Verify if consumers are running.

        Verify if the consumers are running
        Stop all consumers the consumers if detect that
        one or more consumers failed or stop was called

        Returns
        -------
            bool
                True if consumers are running
                False if consumers are stopped
        """
        if self.__started__ is False:
            self.__stop_consumer__(consumers)

        init = False
        one_failed = False
        for consumer in consumers:
            if consumer.is_alive() is True:
                consumer.join(0.01)
            if consumer.is_alive() is True:
                init = True
            else:
                one_failed = True
        if one_failed is True:
            self.__conumers_failed__ = True
            self.__stop_consumer__(consumers)
        return init

    def __wait_consumers_finish__(self, consumers):
        """Wait for all consumers finished."""
        self.logger.info("Waiting consumers finished")
        init = True
        while init is True:
            init = self.__wait_consumer__(consumers)
        self.logger.info("All consumers finished")

    def __wait_producers_finish__(self):
        """Wait for producers finished.

        Wait for util stop was called or the consumers failed
        After that ask to producers stop and wait for they finish
        """
        self.logger.info("Waiting Producers finished")
        while (not self.__conumers_failed__) and self.__started__:
            sleep(0.01)
        for producer in self.__list_topics_send__.values():
            if producer[1] is not None:
                producer[1].stop()
        self.logger.info("All producers finished")

    def run(self):
        """Client main thread.

        Start consumers and producers and then wait they finished
        After that if stop was not called restart all consumers and producers
        if stop was called finish the thread
        """
        self.logger.info("Start App")
        self.__started__ = True

        while self.__started__ is True:
            sleep(0.1)
            consumers = self.__create_consumers__()
            self.__start_consumers__(consumers)
            self.__wait_consumers_finish__(consumers)
            self.__wait_producers_finish__()
        self.logger.info("App finished")

    def producer(self, name, *func_args, **func_kargs):
        """Send a message to a topic.

        Before send a message verify if the consumer is stopped
        if is stopped starts it

        Parameters
        ----------
            name: str
                Name of a function called. It is used just to find
                the right producer to send the message
            func_args: *args
                List of arguments to be passed to producer's method produce
            func_kargs: **kargs
                {key:value} format list to be passed to producer's
                method produce

        Returns
        -------
            bool
                True if the message have been successful sent
                otherwise returns False
        """
        self.logger.debug(f"Send message, function: { name }")
        pbuilder, producer = self.__list_topics_send__[name]
        self.logger.debug(f"Send message to topic: {pbuilder}")
        success = True
        try:
            if producer is None:
                producer = pbuilder.create(self)
                self.__list_topics_send__[name] = (pbuilder, producer)

            producer.produce(*func_args, **func_kargs)
        except (Exception) as exc:
            self.logger.exception(f"Cant send: {pbuilder} "
                                  f"error: {type(exc)} {exc}")
            success = False
        return success

    def stop(self):
        """Ask to Client stop."""
        self.__started__ = False

    def wait(self):
        """Wait the Client stop."""
        while self.is_alive() is True:
            self.join(0.01)

    def is_fineshed(self):
        """Verify if the Client stop.

        Returns
        -------
            bool
                True if Client finished otherwise False
        """
        return self.is_alive() is not True
