from pykafka.exceptions import KafkaException
from pykafka import KafkaClient

class Producer:
    def __init__(self, conargs, conkargs, topic, args, kargs):
        self.__kargs__ = kargs
        self.__args__ = args
        self.__topic__ = topic
        self.__conargs__ = conargs
        self.__conkargs__ = conkargs
        self.__producer__ = None
 
    def produce( self, *func_args, **func_kargs ):
        try:
            if self.__producer__ is None:
                kafka_client = KafkaClient( *self.__conargs__, **self.__conkargs__)
                t = kafka_client.topics[self.__topic__] 
                self.__producer__ = t.get_producer( *self.__args__, **self.__kargs__ )
            self.__producer__.produce( *func_args, **func_kargs )
        except (Exception, KafkaException) as e:
            if self.__producer__ is not None:
                self.__producer__.stop()
                self.__producer__ = None
            raise e
            
    def stop(self):
        if self.__producer__ is not None:
            self.__producer__.stop()
            self.__producer__ = None