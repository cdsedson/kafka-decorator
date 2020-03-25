from pykafka import KafkaClient
from pykafka.exceptions import KafkaException
from threading import Thread

class kafka_client:
    class kafka_client_job(Thread):
        def __init__(self, c, f, cls):
            self.c = c
            self.f = f
            self.cls = cls
            self.init = True
            Thread.__init__(self)

        def run(self):
            while self.init == True:
                try:
                    msg = self.c.consume()
                    if msg is not None:
                        self.f( self.cls, msg)
                except KafkaException as e:
                    print(e)
                    break

        def stop(self):
            if self.init != False:
                self.init = False
                self.c.stop()
            
    def __init__(self, *args, **kargs ):
        self.kafka_args = None
        self.cls = None
        self.list_topics = {}
        self.init = True

    def host(self, *args, **kargs ):
        def inner( Cls ):
            class newCls:
                def __init__(obj, *iargs, **ikargs):
                    self.cls = Cls( *iargs, **ikargs)
            self.kafka_args = ( args, kargs )
            return newCls
        return inner

    def consumer(self, topic, *func_args, **func_kargs ):
        print('chamou1')
        def kafka_client_consumer_inner(f):
            print('chamou2')
            self.list_topics[topic] = (func_args, func_kargs, f )
            print('chamou4')
            return f
        return kafka_client_consumer_inner

    def run(self):
        conargs, conkargs = self.kafka_args
        kafka_client = KafkaClient( *conargs, **conkargs)
        threads = []
        for topic, consumer in self.list_topics.items():
            args, kargs, f = consumer
            print('chamou 5')
            try:
                t = kafka_client.topics[topic]            
                c = t.get_balanced_consumer( *args, **kargs )
                job = self.kafka_client_job( c, f, self.cls )
                threads.append( job )
            except Exception as e:
                print(e)
                break
            except:
                print("Erro desconhecido")
                break

        for t in threads:
            t.start()
        init = True
        while init == True:
            init = False
            for t in threads:
                if t.is_alive() == True: 
                    t.join(0.1)
                if t.is_alive() == False or self.init == False:
                    for st in threads:
                        st.stop()
                    print('finish')
                else:
                    init = True 

    def start(self):
        self.init = True
        while self.init == True:
            try:
                self.run()
            except Exception as e:
                print(e)
            except:
                print("Erro desconhecido")

    def stop(self):
        self.init = False

kc = kafka_client(  )

#@kc.host(zookeeper_hosts='10.142.0.10:9092' )
@kc.host(hosts='10.142.0.10:9092' )
class A:
    def __init__(self):
        print('default')
        self.a = 'teste'
        pass

    @kc.consumer('topicPositionsJSON', consumer_group='testgroup',
                    auto_commit_enable=True, managed=True, consumer_timeout_ms=1000)
    def get(self, msg):
        print(self.a)
        print (msg.value.decode('utf-8'))


import signal, os

def handler(signum, frame):
    print( 'Signal handler called with signal', signum )
    kc.stop()

signal.signal(signal.SIGINT, handler)

print('classe criada')
a = A()
print('objeto criado')
kc.start()
print('acabou')


