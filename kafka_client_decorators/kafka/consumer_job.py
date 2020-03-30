from pykafka.exceptions import KafkaException
from threading import Thread
from pykafka import KafkaClient

class ConsumerJob(Thread):
    def __init__(self, parent, conf  ):#topic, args, kargs, f):
        self.__parent__ = parent
        self.__conf__ = conf
        Thread.__init__(self)

    def __receive__(self, function ):
        for msg in self.__consumer__:
            if msg is not None:
                function( self.__parent__, msg)
            if self.__started__ == False:
                break
    
    def run(self):
        self.__started__ = True
        try:
            conn  = self.__parent__.getConnection()
            kafka_client = KafkaClient( *conn.args, **conn.kargs )
            t = kafka_client.topics[self.__conf__.topic]
            self.__consumer__ = t.get_balanced_consumer( *self.__conf__.args, **self.__conf__.kargs )
            f = self.__conf__.function
            while self.__started__ == True:
                self.__receive__( f )
        except KafkaException as e:
            print(e)
        
            
    def stop(self):
        if self.__started__ != False:
            self.__started__ = False
            self.__consumer__.stop()
