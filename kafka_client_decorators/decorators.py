from .kafka import Client

class KafkaDecorator:
    def __init__(self):
        self.__list_topics__ = {}
        self.__list_topics_send__ = {}
        self.kafka_args = None
        self.cls = None

    def host(self, *args, **kargs ):
        def inner( Cls ):
            class newCls(Client, Cls):
                def __init__(obj, *iargs, **ikargs):
                    Client.__init__( obj, self.__list_topics__ )
                    Cls.__init__( obj, *iargs, **ikargs )
                    obj.decor = self
                    self.cls = obj
            self.kafka_args = ( args, kargs )
            return newCls
        return inner

    def consumer(self, topic, *func_args, **func_kargs ):
        def kafka_client_consumer_inner(f):
            self.__list_topics__[topic] = (func_args, func_kargs, f )
            return f
        return kafka_client_consumer_inner

    def producer(self, topic, *args, **kargs ):
        self.__list_topics_send__[topic] = (args, kargs, None)
        def inner_producer( f ): 
            def inner( obj, *func_args, **func_kargs ):
                return self.cls.producer( topic, *func_args, **func_kargs )
            return inner
        return inner_producer