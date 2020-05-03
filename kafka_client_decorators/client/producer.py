#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Define Producer class."""

from ..util import get_logger


class Producer:
    """Send message to topic.

    Send messages to a topic and manage the producer connection
    """

    def __init__(self, connection):
        """Create a Producer object.

        Parameters
        ----------
            connection: ProducerFactory
                A object capable of create a producer connection
        """
        self.logger = get_logger(__name__)
        self.logger.info(f"Creating Producer for {connection}")
        self.__conn__ = connection
        self.__producer__ = None

    def produce(self, *func_args, **func_kargs):
        """Send message.

        Send a message over a producer coonection, and if needed
        create a producer connection.

        Parameters
        ----------
            func_args: *args
                A list of arguments used by the method produce
                of prodecer connection
            func_kargs: **kargs
                {key:value} format list used by the method produce
                of prodecer connection
        Raises
        ------
            Exception
                It will reraise any exception raised by the
                producer connection when it is sendind a message
        """
        self.logger.debug(f"Send message, {self.__conn__}")
        try:
            if self.__producer__ is None:
                self.__producer__ = self.__conn__.create()
            self.__producer__.produce(*func_args, **func_kargs)
            self.logger.debug(f"Mesage sent for {self.__conn__}")
        except Exception as exc:
            self.logger.exception(f"Exception raised: {exc}")
            if self.__producer__ is not None:
                self.__producer__.stop()
                self.__producer__ = None
            raise exc

    def stop(self):
        """Stop the producer."""
        self.logger.info(f"Stopping Producer: {self.__conn__}")
        if self.__producer__ is not None:
            try:
                self.__producer__.stop()
            except Exception as exc:
                self.logger.exception("Exception on stop listen "
                                      f"{self.__conn__} : {type(exc)} {exc}")
            self.__producer__ = None
        self.logger.debug(f"producer Stoped: {self.__conn__}")
