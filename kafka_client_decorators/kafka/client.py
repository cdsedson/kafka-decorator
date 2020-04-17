#!/usr/bin/python
# -*- coding: <encoding name> -*-

from .consumer_job import ConsumerJob 
from .producer import Producer
from .logging_helper import get_logger
from time import sleep

from threading import Thread

class Client(Thread):
    def __init__(self, connection_args, list_topics_receive, list_topics_send ):
        self.logger = get_logger(__name__)
        self.logger.info( f"Creating cliet, listen topics: {[t.topic for t in list_topics_receive]} send topics: {[t.topic for t in list_topics_send]}" )
        
        self.__started__  = True
        self.__conumers_failed__ = False
        self.__list_topics_receive__ = list_topics_receive
        self.__connection_args__ = connection_args 
        self.__list_topics_send__ = { p.topic: (p, None) for p in list_topics_send }
        Thread.__init__(self)

    def getConnection(self):
        return self.__connection_args__
        
    def __createConsumers__( self ):
        self.logger.info( "Creating consumers" )
        consumers = []
        
        for conf in self.__list_topics_receive__:
            job = ConsumerJob( self, conf )
            consumers.append( job )
        self.__conumers_failed__ = False
        return consumers
        
    def __startConsumers__( self, consumers ):
        self.logger.info( f"Staring consumers" )
        for c in consumers:
            c.start()
    
    def __stopConsumer__( self, consumers ):
        self.logger.info( f"Stopping consumers" )
        for st in consumers:
            if st is not None:
                st.stop()
            
    def __waitConsumer__( self, consumers ):
        if self.__started__  is False:
            self.__stopConsumer__( consumers )
        
        init = False
        one_failed = False
        for t in consumers:
            if t.is_alive() is True: 
                t.join(0.01)
            if t.is_alive() is True:
                init = True
            else:
                one_failed = True
        if one_failed == True:
            self.__conumers_failed__ = True
            self.__stopConsumer__( consumers )
        return init
                        
    def __waitConsumersFinish__( self, consumers ):
        self.logger.info( f"Waiting consumers finished" )
        init = True
        while init is True:
            init = self.__waitConsumer__( consumers )
        self.logger.info( f"All consumers finished" )
            
    def __waitProducersFinish__( self ):
        self.logger.info( f"Waiting Producers finished" )
        while (not self.__conumers_failed__) and self.__started__ :
            sleep(0.01)
        for p in self.__list_topics_send__.values():
            if p[1] is not None:
                p[1].stop()
        self.logger.info( f"All producers finished" )
                          
    def run(self):
        self.logger.info( f"Start App" )
        self.__started__  = True
        
        while self.__started__  == True:
            sleep(0.1)
            consumers = self.__createConsumers__( )
            self.__startConsumers__( consumers )
            self.__waitConsumersFinish__( consumers )
            self.__waitProducersFinish__( )
        self.logger.info( f"App finished" )

    def producer(self, topic, *func_args, **func_kargs ):
        self.logger.debug( f"Send message to topic{ topic }" )
        pconf, p = self.__list_topics_send__[topic]
        success = True
        try:
            if p is None: 
                p = Producer( self, pconf )
                self.__list_topics_send__[topic] = (pconf, p)
                
            p.produce( *func_args, **func_kargs )
        except (Exception) as e:
            self.logger.exception( f"Cant send topic: {topic} error: {type(e)} {e}" )
            success = False
        return success

    def stop(self):
        self.__started__  = False
        
    def wait(self):
        while self.is_alive( ) is True:
            self.join( 0.01 )
        
    def is_fineshed(self):
        return self.is_alive( ) != True
