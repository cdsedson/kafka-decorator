#!/usr/bin/python
# -*- coding: <encoding name> -*-

from pykafka.exceptions import KafkaException
from pykafka.exceptions import ConsumerStoppedException
from threading import Thread
from pykafka import KafkaClient
from .consumer_parameter import ConsumerParmeters
from .logging_helper import getLogger

class ConsumerJob(Thread):
    def __init__(self, parent, conf  ):
        self.logger = getLogger(__name__)
        self.logger.info( f"Creating Consumer for topic: {conf.topic}" )
        
        self.__consumer__ = None
        self.__parent__ = parent
        self.__conf__ = conf
        Thread.__init__(self)

    def __receive__(self, function ):
        self.logger.debug( f"Start receive, topic: {self.__conf__.topic}" )
        for msg in self.__consumer__:
            if msg is not None:
                function( self.__parent__, msg)
                self.logger.debug( f"Message received from topic: {self.__conf__.topic}: offset: {msg.offset}" )
            if self.__started__ == False:
                self.logger.info( f"Asked to stop, topic: {self.__conf__.topic}" )
                break
        self.logger.debug( f"Stop receive, topic: {self.__conf__.topic}" )
    
    def __listen__(self):
        self.logger.info( f"Start listen, topic: {self.__conf__.topic}" )
        try:
            conn  = self.__parent__.getConnection()
            kafka_client = KafkaClient( *conn.args, **conn.kargs )
            t = kafka_client.topics[self.__conf__.topic]
            if self.__conf__.kind == ConsumerParmeters.BALANCED:
                self.logger.info( f"Create balanced consumer to topic: {self.__conf__.topic}" )
                self.__consumer__ = t.get_balanced_consumer( *self.__conf__.args, **self.__conf__.kargs )
            else:
                self.logger.info( f"Create simple consumer to topic: {self.__conf__.topic}" )
                self.__consumer__ = t.get_simple_consumer( *self.__conf__.args, **self.__conf__.kargs )
            f = self.__conf__.function
            while self.__started__ == True:
                self.__receive__( f )
        except ConsumerStoppedException as e:
            self.logger.debug( f"Exception from topic: {self.__conf__.topic} : {type(e)} {e}" )
            self.__started__ = False 
        except KafkaException as e:
            self.logger.exception( f"Exception from topic: {self.__conf__.topic} : {type(e)} {e}" )
            
        self.logger.info( f"Stop listen, topic: {self.__conf__.topic}" )
        
    def run(self):
        self.__started__ = True
        try:
            self.__listen__()
        except KafkaException as e:
            self.logger.exception( f"Exception from topic, when handling another: {self.__conf__.topic} : {type(e)} {e}" )
        except:
            self.logger.exception( f"Exception from topic, when handling another: {self.__conf__.topic}" )
            
        self.stop()
          
    def stop(self):
        self.logger.info( f"Stopping consumer, topic: {self.__conf__.topic}" )
        if self.__started__ == True:
            self.__started__ = False
            try:
                if self.__consumer__ is not None:
                    self.__consumer__.stop()
                self.logger.debug( f"Stoped consumer, topic: {self.__conf__.topic}" )
            except KafkaException as e:
                self.logger.exception( f"Exception on stop listen topic: {self.__conf__.topic} : {type(e)} {e}" )
            
