from .kafka import Client

class Parameters:
    def __init__( self, args, kargs ):
        self.args = args
        self.kargs = kargs

class KafkaDecorator:
    def __init__(self):
        self.__topics_receive__ = {}
        self.__topics_send__ = {}
        self.cls = None

    def host(self, *args, **kargs ):
        def inner( Cls ):
            class newCls(Client, Cls):
                def __init__(obj, *iargs, **ikargs):
                    connection = Parameters( args, kargs ) 
                    Client.__init__( obj, connection, self.__topics_receive__, self.__topics_send__ )
                    Cls.__init__( obj, *iargs, **ikargs )
                    self.cls = obj
            return newCls
        return inner

    def consumer(self, topic, *func_args, **func_kargs ):
        def kafka_client_consumer_inner(f):
            self.__topics_receive__[topic] = (func_args, func_kargs, f )
            return f
        return kafka_client_consumer_inner

    def producer(self, topic, *args, **kargs ):
        self.__topics_send__[topic] = (args, kargs)
        def inner_producer( f ): 
            def inner( obj, *func_args, **func_kargs ):
                return self.cls.producer( topic, *func_args, **func_kargs )
            return inner
        return inner_producer