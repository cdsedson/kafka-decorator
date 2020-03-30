from .kafka import Client

class ConnectionParmeters:
    def __init__( self, args, kargs ):
        self.args = args
        self.kargs = kargs

class ConsumerParmeters:
    def __init__( self, topic, args, kargs, function ):
        self.args = args
        self.kargs = kargs
        self.function = function
        self.topic = topic

        
class KafkaDecorator:
    def __init__(self):
        self.__topics_receive__ = []
        self.__topics_send__ = {}
        self.__connection__ = None
        self.cls = None

    def host(self, *args, **kargs ):
        def inner( Cls ):
            class newCls(Client, Cls):
                def __init__(obj, *iargs, **ikargs):
                    Client.__init__( obj, self.__connection__, self.__topics_receive__, self.__topics_send__ )
                    Cls.__init__( obj, *iargs, **ikargs )
                    self.cls = obj
            return newCls
        self.__connection__ = ConnectionParmeters( args, kargs )
        return inner

    def consumer(self, topic, *func_args, **func_kargs ):
        def kafka_client_consumer_inner(f):
            cConf = ConsumerParmeters( topic, func_args, func_kargs, f )
            self.__topics_receive__.append( cConf )
            return f
        return kafka_client_consumer_inner

    def producer(self, topic, *args, **kargs ):
        self.__topics_send__[topic] = (args, kargs)
        def inner_producer( f ): 
            def inner( obj, *func_args, **func_kargs ):
                return self.cls.producer( topic, *func_args, **func_kargs )
            return inner
        return inner_producer