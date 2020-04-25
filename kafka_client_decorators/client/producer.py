#!/usr/bin/python3
# -*- coding: utf-8 -*-

from ..util import get_logger


class Producer:
    def __init__(self, conn):
        self.logger = get_logger(__name__)
        self.logger.info(f"Creating Producer for {conn}")
        self.__conn__ = conn
        self.__producer__ = None

    def produce(self, *func_args, **func_kargs):
        self.logger.debug(f"Send message, {self.__conn__}")
        try:
            if self.__producer__ is None:
                self.__producer__ = self.__conn__.create()
            self.__producer__.produce(*func_args, **func_kargs)
            self.logger.debug(f"Mesage sent for {self.__conn__}")
        except Exception as e:
            self.logger.exception(f"Exception raised: {e}")
            if self.__producer__ is not None:
                self.__producer__.stop()
                self.__producer__ = None
            raise e

    def stop(self):
        self.logger.info(f"Stopping Producer: {self.__conn__}")
        if self.__producer__ is not None:
            try:
                self.__producer__.stop()
            except Exception as e:
                self.logger.exception("Exception on stop listen "
                                      f"{self.__conn__} : {type(e)} {e}")
            self.__producer__ = None
        self.logger.debug(f"producer Stoped: {self.__conn__}")
