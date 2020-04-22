#!/usr/bin/python3
# -*- coding: utf-8 -*-

from pykafka.exceptions import KafkaException
from pykafka.exceptions import ConsumerStoppedException
from threading import Thread
from .consumer_parameter import ConsumerParmeters
from .logging_helper import get_logger
from .consumer_factory import ConsumerFactory


class ConsumerJob(Thread):
    def __init__(self, parent, conf):
        self.logger = get_logger(__name__)
        self.logger.info(f"Creating Consumer for topic: {conf.topic}")

        self.__consumer__ = None
        self.__parent__ = parent
        self.__conf__ = conf
        Thread.__init__(self)

    def __receive__(self, function):
        self.logger.debug(f"Start receive, topic: {self.__conf__.topic}")
        for msg in self.__consumer__:
            if msg is not None:
                function(self.__parent__, msg)
                self.logger.debug("Message received from topic: "
                                  f"{self.__conf__.topic}: "
                                  f"offset: {msg.offset}")
            if self.__started__ is False:
                self.logger.info("Asked to stop, topic: "
                                 f"{self.__conf__.topic}")
                break
        self.logger.debug(f"Stop receive, topic: {self.__conf__.topic}")

    def __listen__(self):
        self.logger.info(f"Start listen, topic: {self.__conf__.topic}")
        try:
            conn = self.__parent__.getConnection()
            if self.__conf__.kind == ConsumerParmeters.BALANCED:
                self.logger.info("Create balanced consumer to topic: "
                                 f"{self.__conf__.topic}")
                self.__consumer__ = ConsumerFactory.get_consumer_balanced(
                    conn, self.__conf__)
            else:
                self.logger.info("Create simple consumer to topic: "
                                 f"{self.__conf__.topic}")
                self.__consumer__ = ConsumerFactory.get_consumer_simple(
                    conn, self.__conf__)
            f = self.__conf__.function
            while self.__started__ is True:
                self.__receive__(f)
        except ConsumerStoppedException as e:
            self.logger.debug("Exception from topic: "
                              f"{self.__conf__.topic} : {type(e)} {e}")

        self.logger.info(f"Stop listen, topic: {self.__conf__.topic}")

    def run(self):
        self.__started__ = True
        try:
            self.__listen__()
        except (Exception, KafkaException) as e:
            self.logger.exception("Exception from topic: "
                                  f"{self.__conf__.topic} : {type(e)} {e}")

    def stop(self):
        self.logger.info(f"Stopping consumer, topic: {self.__conf__.topic}")
        if self.__started__ is True:
            self.__started__ = False
            try:
                if self.__consumer__ is not None:
                    self.__consumer__.stop()
                self.logger.debug("Stoped consumer, "
                                  f"topic: {self.__conf__.topic}")
            except Exception as e:
                self.logger.exception("Exception on stop listen topic: "
                                      f"{self.__conf__.topic} : {type(e)} {e}")
