from kafka_client_decorators import KafkaDecorator

kc = KafkaDecorator(  )

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


