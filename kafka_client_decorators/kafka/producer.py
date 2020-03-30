from pykafka.exceptions import KafkaException
from pykafka import KafkaClient

class Producer:
    def __init__(self, parent, conf):
        self.__parent__ = parent
        self.__conf__ = conf
        self.__producer__ = None
 
    def produce( self, *func_args, **func_kargs ):
        try:
            if self.__producer__ is None:
                conn =  self.__parent__.getConnection()
                kafka_client = KafkaClient( *conn.args, **conn.kargs)
                t = kafka_client.topics[self.__conf__.topic] 
                self.__producer__ = t.get_producer( *self.__conf__.args, **self.__conf__.kargs )
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