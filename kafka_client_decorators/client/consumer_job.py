#!/usr/bin/python3
# -*- coding: utf-8 -*-

from threading import Thread
from ..util import get_logger


class ConsumerJob(Thread):

    def __init__(self, parent, function, conn):
        self.logger = get_logger(__name__)
        self.logger.info(f"Creating Consumer for {conn}")

        self.__consumer__ = None
        self.__parent__ = parent
        self.__function__ = function
        self.__conn__ = conn
        Thread.__init__(self)

    def __receive__(self, function):
        self.logger.debug(f"Start receive, {self.__conn__}")
        for msg in self.__consumer__:
            if msg is not None:
                function(self.__parent__, msg)
                self.logger.debug("Message received from "
                                  f"{self.__conn__}: "
                                  f"offset: {msg.offset}")
            if self.__started__ is False:
                self.logger.info(f"Asked to stop, {self.__conn__}")
                break
        self.logger.debug(f"Stop receive, topic: {self.__conn__}")

    def __listen__(self):
        self.logger.info(f"Start listen, {self.__conn__}")
        self.__consumer__ = self.__conn__.create()
        f = self.__function__
        while self.__started__ is True:
            self.__receive__(f)
        self.logger.info(f"Stop listen, {self.__conn__}")

    def run(self):
        self.__started__ = True
        try:
            self.__listen__()
        except Exception as e:
            self.logger.exception("Exception from "
                                  f"{self.__conn__} : {type(e)} {e}")

    def stop(self):
        self.logger.info(f"Stopping consumer, topic: {self.__conn__}")
        if self.__started__ is True:
            self.__started__ = False
            try:
                if self.__consumer__ is not None:
                    self.__consumer__.stop()
                self.logger.debug(f"Stoped consumer, {self.__conn__}")
            except Exception as e:
                self.logger.exception("Exception on stop listen "
                                      f"{self.__conn__} : {type(e)} {e}")
