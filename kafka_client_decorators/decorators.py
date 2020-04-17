#!/usr/bin/python
# -*- coding: <encoding name> -*-

from .kafka import Client
from .kafka.connection_parameter import ConnectionParmeters
from .kafka.consumer_parameter import ConsumerParmeters
from .kafka.producer_parameter import ProducerParmeters
from .kafka.logging_helper import get_logger

class KafkaDecorator:
    def __init__(self):
        self.logger = get_logger(__name__)
        self.logger.info( "Creating decorator " )
        
        self.__topics_receive__ = []
        self.__topics_send__ = []
        self.__connection__ = None
        self.cls = None

    def host(self, *args, **kargs ):
        self.logger.info( f"Adding host" )
        def inner( cls ):
            class NewCls(Client, cls):
                def __init__(obj, *iargs, **ikargs):
                    self.logger.info( f"Creating class: {cls}" )
                    Client.__init__( obj, self.__connection__, self.__topics_receive__, self.__topics_send__ )
                    cls.__init__( obj, *iargs, **ikargs )
                    self.cls = obj
            return NewCls
        self.__connection__ = ConnectionParmeters( args, kargs )
        return inner

    def balanced_consumer(self, topic, *func_args, **func_kargs ):
        self.logger.info( f"Adding balanced consumer, topic: {topic}" )
        def kafka_client_consumer_inner(f):
            c_conf = ConsumerParmeters( ConsumerParmeters.BALANCED, topic, func_args, func_kargs, f )
            self.__topics_receive__.append( c_conf )
            return f
        return kafka_client_consumer_inner
        
    def simple_consumer(self, topic, *func_args, **func_kargs ):
        self.logger.info( f"Adding simple consumer, topic: {topic}" )
        def kafka_client_consumer_inner(f):
            c_conf = ConsumerParmeters( ConsumerParmeters.SIMPLE, topic, func_args, func_kargs, f )
            self.__topics_receive__.append( c_conf )
            return f
        return kafka_client_consumer_inner

    def producer(self, topic, *args, **kargs ):
        self.logger.info( f"Adding producer, topic: {topic}" )
        p_conf = ProducerParmeters(topic, args, kargs)
        self.__topics_send__.append( p_conf )
        def inner_producer( f ): 
            def inner( obj, *func_args, **func_kargs ):
                return self.cls.producer( topic, *func_args, **func_kargs )
            return inner
        return inner_producer