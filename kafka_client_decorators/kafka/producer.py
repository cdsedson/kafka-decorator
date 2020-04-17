#!/usr/bin/python
# -*- coding: <encoding name> -*-

from .producer_factory import ProducerFactory
from .logging_helper import get_logger

class Producer:
    def __init__(self, parent, conf):
        self.logger = get_logger(__name__)
        self.logger.info( f"Creating Producer for topic: {conf.topic}" )
        self.__parent__ = parent
        self.__conf__ = conf
        self.__producer__ = None
 
    def produce( self, *func_args, **func_kargs ):
        self.logger.debug(f"Send message: {self.__conf__.topic}" )
        try:
            if self.__producer__ is None:
                self.logger.debug(f"Creating kafka producer: {self.__conf__.topic}" )
                conn =  self.__parent__.getConnection()
                self.__producer__ = ProducerFactory.get_producer( conn, self.__conf__)
            self.__producer__.produce( *func_args, **func_kargs )
            self.logger.debug(f"Mesage sent for topic: {self.__conf__.topic}" )
        except Exception as e:
            self.logger.exception(f"Exception raised: {e}" )
            if self.__producer__ is not None:
                self.__producer__.stop()
                self.__producer__ = None
            raise e
            
    def stop(self):
        self.logger.info( f"Stopping Producer: {self.__conf__.topic}" )
        if self.__producer__ is not None:
            try:
                self.__producer__.stop()
            except Exception as e:
                self.logger.exception( f"Exception on stop listen topic: {self.__conf__.topic} : {type(e)} {e}" )
            self.__producer__ = None
        self.logger.debug( f"producer Stoped: {self.__conf__.topic}" )