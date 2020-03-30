from pykafka.exceptions import KafkaException
from threading import Thread
from pykafka import KafkaClient

class ConsumerJob(Thread):
    def __init__(self, cls, connection, topic, args, kargs, f):
        self.__kargs__ = kargs
        self.__args__ = args
        self.__f__ = f
        self.__cls__ = cls
        self.__topic__ = topic
        self.__started__ = None
        self.__connection__ = connection
        self.__consumer__ = None
        Thread.__init__(self)

    def __receive__(self):
        for msg in self.__consumer__:
            if msg is not None:
                self.__f__( self.__cls__, msg)
            if self.__started__ == False:
                break
    
    def run(self):
        self.__started__ = True
        try:
            kafka_client = KafkaClient( *self.__connection__.args, **self.__connection__.kargs )
            t = kafka_client.topics[self.__topic__]
            self.__consumer__ = t.get_balanced_consumer( *self.__args__, **self.__kargs__ )
            while self.__started__ == True:
                self.__receive__()
        except KafkaException as e:
            print(e)
        
            
    def stop(self):
        if self.__started__ != False:
            self.__started__ = False
            self.__consumer__.stop()
