#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Define a ConsumerJob class."""

from threading import Thread

from ..util import get_logger


class ConsumerJob(Thread):
    """Receive message from a topic.

    Receive messages to a topic and manage the consumer connection
    """

    def __init__(self, parent, function, connection):
        """Create a ConsumerJob class.

        Parameters
        ----------
            parent: object
                A object from the function will be called
            function: f(message: msg) -> any
                A function from the object parent that receive a message
                as parameter
            connection: ConsumerFactory
                A object capable of create a consumer connection
        """
        self.__logger = get_logger(__name__)
        self.__logger.info(f"Creating Consumer for {connection}")

        self.__consumer__ = None
        self.__parent__ = parent
        self.__function__ = function
        self.__conn__ = connection
        self.__started__ = False
        Thread.__init__(self)

    def __receive__(self):
        """Receive a message.

        Read a message from the consumer connection and call a
        function of the object parent passing the message as
        parameter
        If nedeed create a consumer coonection
        """
        function = self.__function__
        self.__logger.debug(f"Start receive, {self.__conn__}")
        for msg in self.__consumer__:
            if msg is not None:
                function(self.__parent__, msg)
                self.__logger.debug("Message received from "
                                    f"{self.__conn__}: "
                                    f"offset: {msg.offset}")
            if self.__started__ is False:
                self.__logger.info(f"Asked to stop, {self.__conn__}")
                break
        self.__logger.debug(f"Stop receive, topic: {self.__conn__}")

    def __listen__(self):
        """Listen messages.

        While the consumer is acive call the method self.__receive__
        how many times id needed
        """
        self.__logger.info(f"Start listen, {self.__conn__}")
        self.__consumer__ = self.__conn__.create()
        while self.__started__ is True:
            self.__receive__()
        self.__logger.info(f"Stop listen, {self.__conn__}")

    def run(self):
        """Start of the ConsumerJob thread."""
        self.__started__ = True
        try:
            self.__listen__()
        except Exception as exc:
            excstr = f"Exception from {self.__conn__}: {type(exc)} {exc}"
            if self.__started__ is True:
                self.__logger.exception(excstr)
            else:
                self.__logger.debug(excstr)

    def stop(self):
        """Stop the conumer thread."""
        self.__logger.info(f"Stopping consumer, topic: {self.__conn__}")
        if self.__started__ is True:
            self.__started__ = False
            try:
                if self.__consumer__ is not None:
                    self.__consumer__.stop()
                self.__logger.debug(f"Stoped consumer, {self.__conn__}")
            except Exception as exc:
                self.__logger.exception("Exception on stop listen "
                                        f"{self.__conn__} : {type(exc)} {exc}")
