from pykafka import KafkaClient
from pykafka.exceptions import KafkaException
from threading import Thread
from time import sleep

class kafka_consumer_job(Thread):
    def __init__(self, cls, conargs, conkargs, topic, args, kargs, f):
        self.kargs = kargs
        self.args = args
        self.f = f
        self.cls = cls
        self.topic = topic
        self.init = None
        self.conargs = conargs
        self.conkargs = conkargs
        self.c = None
        Thread.__init__(self)

    def receive(self):
        for msg in self.c:
            if msg is not None:
                self.f( self.cls, msg)
            if self.init == False:
                break
    
    def run(self):
        self.init = True
        try:
            kafka_client = KafkaClient( *self.conargs, **self.conkargs)
            t = kafka_client.topics[self.topic]
            self.c = t.get_balanced_consumer( *self.args, **self.kargs )
            while self.init == True:
                self.receive()
        except KafkaException as e:
            print(e)
        
            
    def stop(self):
        if self.init != False:
            self.init = False
            self.c.stop()
            
class kafka_producer:
    def __init__(self, conargs, conkargs, topic, args, kargs):
        self.kargs = kargs
        self.args = args
        self.topic = topic
        self.conargs = conargs
        self.conkargs = conkargs
        self.p = None
        self.kargs = kargs
        self.args = args
 
    def produce( self, *func_args, **func_kargs ):
        try:
            if self.p is None:
                kafka_client = KafkaClient( *self.conargs, **self.conkargs)
                t = kafka_client.topics[self.topic] 
                self.p = t.get_producer( *self.args, **self.kargs )
            self.p.produce( *func_args, **func_kargs )
        except (Exception, KafkaException) as e:
            if self.p is not None:
                self.p.stop()
                self.p = None
            raise e
            
    def stop(self):
        if self.p is not None:
            self.p.stop()
            self.p = None

class kafka_client(Thread):
    def __init__(self, *args, **kargs ):
        self.init = True
        self.finished = False
        Thread.__init__(self)

    def createConsumers( self ):
        conargs, conkargs = self.decor.kafka_args
        consumers = []
        for topic, consumer in self.decor.list_topics.items():
            args, kargs, f = consumer
            try:
                job = kafka_consumer_job( self.decor.cls, conargs, conkargs, topic, args, kargs, f )
                consumers.append( job )
            except Exception as e:
                print(e)
                consumers = []
                break
            except:
                print("Erro desconhecido")
                consumers = []
                break
        return consumers
        
    def startConsumers( self, consumers ):
        for c in consumers:
            c.start()
    
    def stopConsumer( self, consumers ):
        for st in consumers:
            st.stop()
            
    def waitConsumer( self, consumers ):
        if self.init is False:
            self.stopConsumer( consumers )
        
        init = False
        for t in consumers:
            if t.is_alive() is True: 
                t.join(0.1)
            if t.is_alive() is True:
                init = True
            else:
                self.init = False
        return init
                        
    def waitConsumersFinish( self, consumers ):
        init = True
        while init is True:
            init = self.waitConsumer( consumers )
            
    def waitProducersFinish( self ):
        init = True
        while self.init is True:
            time.sleep(0.01)
        for p in self.decor.list_topics_send.values():
            if p[2] is not None:
                p[2].stop()
                          
    def run(self):
        self.init = True
        
        while self.init == True:
            try:
                conargs, conkargs = self.decor.kafka_args
                consumers = self.createConsumers( )
                self.startConsumers( consumers )
                self.waitConsumersFinish( consumers )
                self.waitProducersFinish( )
            except Exception as e:
                print(e)
            except:
                print("Erro desconhecido")

    def producer(self, topic, *func_args, **func_kargs ):
        args, kargs, p = self.decor.list_topics_send[topic]
        try:
            if p is None:
                conargs, conkargs = self.decor.kafka_args
                p = kafka_producer( conargs, conkargs, topic, args, kargs )
                self.decor.list_topics_send[topic] = (args, kargs, p)
                
            p.produce( *func_args, **func_kargs )
        except (Exception) as e:
            raise e

    def stop(self):
        self.init = False
        
    def wait(self):
        while self.is_alive( ) is True:
            self.join( 0.01 )
        
    def is_fineshed(self):
        return self.is_alive( ) != True

class kafka_client_decorator:
    def __init__(self):
        self.list_topics = {}
        self.list_topics_send = {}
        self.kafka_args = None
        self.cls = None

    def host(self, *args, **kargs ):
        def inner( Cls ):
            class newCls(kafka_client, Cls):
                def __init__(obj, *iargs, **ikargs):
                    kafka_client.__init__( obj )
                    Cls.__init__( obj, *iargs, **ikargs )
                    obj.decor = self
                    self.cls = obj
            self.kafka_args = ( args, kargs )
            return newCls
        return inner

    def consumer(self, topic, *func_args, **func_kargs ):
        def kafka_client_consumer_inner(f):
            self.list_topics[topic] = (func_args, func_kargs, f )
            return f
        return kafka_client_consumer_inner

    def producer(self, topic, *args, **kargs ):
        self.list_topics_send[topic] = (args, kargs, None)
        def inner_producer( f ): 
            def inner( obj, *func_args, **func_kargs ):
                return self.cls.producer( topic, *func_args, **func_kargs )
            return inner
        return inner_producer
	

kc = kafka_client_decorator(  )

#@kc.host(zookeeper_hosts='10.142.0.10:2181' )
@kc.host(hosts='10.142.0.10:9092' )
class A:
    def __init__(self):
        print('default')
        self.a = 'teste'
        pass

    @kc.consumer('topicPositionsJSON', consumer_group='testgroup3', auto_commit_enable=True, managed=True, consumer_timeout_ms=1000)
    def get(self, msg):
        #print(self.a)
        #print (msg.value.decode('utf-8'))
        print ( 'um: ',msg.offset )
        self.send2( msg.value, "chave_2".encode('utf-8') )

    @kc.consumer('teste2', consumer_group='testgroup4', auto_commit_enable=True, managed=True, consumer_timeout_ms=1000)
    def get2(self, msg):
        #print(self.a)
        print ( 'dois: ',msg.offset )
        self.send( msg.value )

    @kc.producer('teste')
    def send(self, msg):
        pass

    @kc.producer('teste2')
    def send2(self, msg, key):
        pass

print('classe criada')
a = A()
import signal, os

def handler(signum, frame):
    print( 'Signal handler called with signal', signum )
    a.stop()

signal.signal(signal.SIGINT, handler)

#print('objeto criado')
a.start()
a.wait()

print('acabou')


