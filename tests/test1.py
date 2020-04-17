import unittest
import mock
import time
from unittest.mock import MagicMock
from pykafka.exceptions import KafkaException
from pykafka.exceptions import ConsumerStoppedException

try:
    print('trying installed module')
    from kafka_client_decorators import KafkaDecorator
except:
    print('installed module failed, trying from path')
    import sys
    sys.path.insert(1, '../')
    from kafka_client_decorators import KafkaDecorator

#from kafka_client_decorators.kafka.logging_helper import setDebugLevel
import logging

#logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
#    datefmt='%Y-%m-%d:%H:%M:%S',
#    level=logging.DEBUG)
    
#logging.basicConfig(level=logging.DEBUG)

#setDebugLevel(logging.INFO)



class helper_kafka:

    def __init__(self):
        self.stop_execpt = False
        self.read = []
    
    def produce(self, *args, **kargs):
        if args[0] == 'Except'.encode('utf-8'):
            self.message  = None
            self.key =  None
            raise Exception("Error in send")
        self.message =  args[0]
        self.key = kargs['partition_key'] if 'partition_key' in kargs else None
    
    def stopException( self ):
        self.stop_execpt = True 
        
    def stop(self):
        if self.stop_execpt is True:
            self.stop_execpt = False
            raise Exception( "Stop error" )
    
    class M:
        def __init__( self, offset, value, partition_key ):
            self.offset = offset
            self.value = value
            self.partition_key = partition_key
        def get(self):
            return self
    class E:
        def __init__( self, excepion ):
            self.excp = excepion

        def get(self):
            raise self.excp
            
    def cleanMessage( self  ):
        self.read = []
        
    def setMessage( self, offset, message, key  ):
        self.read.append( self.M(offset, message, key ) )
        
    def setException( self, excepion  ):
        self.read.append( self.E( excepion ) )
    
    def __iter__(self):
        return self
    
    def __next__(self):
        time.sleep(0.1)
        if len(self.read) <= 0:
            raise StopIteration
        message = self.read.pop(0)
        return message.get()
          

kc = KafkaDecorator(  )
#@kc.host(zookeeper_hosts='localhost:2181' )
@kc.host(hosts='localhost:9092' )
class A:
    def __init__(self):
        pass

    @kc.producer('test1')
    def sendKey(self, msg, key ):
        pass
        
    @kc.producer('test1')
    def send(self, msg ):
        pass
 
kc2 = KafkaDecorator(  )
@kc2.host(hosts='localhost:9092' )
class B:
    def __init__(self):
        self.read = []
    
    @kc2.balanced_consumer('test1', consumer_group='testgroup3')
    def get(self, msg):
        self.read.append(msg)

kc3 = KafkaDecorator(  )
@kc3.host(hosts='localhost:9092' )
class C:
    def __init__(self):
        self.read = []
    
    @kc3.simple_consumer('test1', consumer_group='testgroup3')
    def get(self, msg):
        self.read.append(msg)
#from kafka_client_decorators.kafka import ProducerFactory


class Test1(unittest.TestCase):
    kh = helper_kafka()
        
    @mock.patch( 'kafka_client_decorators.kafka.ProducerFactory.getProducer', return_value=kh )
    def test_send_key(self, Mockkafka ):
        
        a = A()
        a.start()

        a.sendKey( 'Hello'.encode('utf-8'), partition_key='world'.encode('utf-8') )
        
        time.sleep(0.5)
        a.stop()
        a.wait()

        assert self.kh.message == 'Hello'.encode('utf-8') 
        assert self.kh.key == 'world'.encode('utf-8') 
        
    @mock.patch( 'kafka_client_decorators.kafka.ProducerFactory.getProducer', return_value=kh )
    def test_send(self, Mockkafka ):
        
        a = A()
        a.start()

        a.sendKey( 'Hello'.encode('utf-8') )
        a.stop()
        a.wait()
 
        assert self.kh.message == 'Hello'.encode('utf-8') 
        assert self.kh.key == None
        
    @mock.patch( 'kafka_client_decorators.kafka.ProducerFactory.getProducer', return_value=kh )
    def test_exception(self, Mockkafka ):
        
        a = A()
        a.start()
        
        a.sendKey( 'Except'.encode('utf-8'), partition_key='Except'.encode('utf-8') )
        assert self.kh.message is None
        assert self.kh.key is None 
        
        a.sendKey( 'Hello'.encode('utf-8'), partition_key='world'.encode('utf-8') )
        a.stop()
        a.wait()

        assert self.kh.message == 'Hello'.encode('utf-8') 
        assert self.kh.key == 'world'.encode('utf-8') 
        
    @mock.patch( 'kafka_client_decorators.kafka.ProducerFactory.getProducer', return_value=kh )
    def test_stop_exception(self, Mockkafka ):
        
        self.kh.stopException()
        
        a = A()
        a.start()
        
        a.sendKey( 'Erro'.encode('utf-8'), partition_key='Erro'.encode('utf-8') )
        assert self.kh.message == 'Erro'.encode('utf-8') 
        assert self.kh.key == 'Erro'.encode('utf-8')
        assert self.kh.stop_execpt is True
        
        a.stop()
        a.wait()
        
        assert self.kh.stop_execpt is False
        
        a = A()
        a.start()
        
        a.sendKey( 'Erro'.encode('utf-8'), partition_key='Erro'.encode('utf-8') )
        assert self.kh.message == 'Erro'.encode('utf-8') 
        assert self.kh.key == 'Erro'.encode('utf-8')
        
        a.stop()
        a.wait()
        
        assert self.kh.stop_execpt is False
        
    @mock.patch( 'kafka_client_decorators.kafka.ConsumerFactory.get_consumer_balanced', return_value=kh )
    def test_receive(self, Mockkafka ):
        self.kh.cleanMessage(  )
        self.kh.setMessage( 1, 'Hello'.encode('utf-8'), 'world'.encode('utf-8') )
        
        b = B()
        
        b.start()
        b.stop()
        b.wait()

        assert b.read[0].offset == 1
        assert b.read[0].value == 'Hello'.encode('utf-8')
        assert b.read[0].partition_key == 'world'.encode('utf-8')
        
    @mock.patch( 'kafka_client_decorators.kafka.ConsumerFactory.get_consumer_simple', return_value=kh )
    def test_receive_simple(self, Mockkafka ):
        self.kh.cleanMessage(  )
        self.kh.setMessage( 1, 'Hello'.encode('utf-8'), 'world'.encode('utf-8') )
        
        c = C()
        
        c.start()
        c.stop()
        c.wait()
        
        assert c.read[0].offset == 1
        assert c.read[0].value == 'Hello'.encode('utf-8')
        assert c.read[0].partition_key == 'world'.encode('utf-8')
        
    @mock.patch( 'kafka_client_decorators.kafka.ConsumerFactory.get_consumer_balanced', return_value=kh )
    def test_receive_exception(self, Mockkafka ):
        self.kh.cleanMessage(  )
        self.kh.setMessage( 1, 'Hello'.encode('utf-8'), 'world'.encode('utf-8') )
        self.kh.setMessage( 2, 'Hello2'.encode('utf-8'), 'world2'.encode('utf-8') )
        self.kh.setException( ConsumerStoppedException()  )
        self.kh.setMessage( 3, 'Hello3'.encode('utf-8'), 'world3'.encode('utf-8') )
        
        b = B()
        
        b.start()
        start = time.time()
        while time.time() < (start + 30):
            if len( b.read ) == 3:
                break
            time.sleep(0.01)
        
        timeout = time.time() > (start + 30)
        
        b.stop()
        b.wait()
        
        assert not timeout
        if timeout :
            return
        
        assert b.read[0].offset == 1
        assert b.read[0].value == 'Hello'.encode('utf-8')
        assert b.read[0].partition_key == 'world'.encode('utf-8')
        
        assert b.read[1].offset == 2
        assert b.read[1].value == 'Hello2'.encode('utf-8')
        assert b.read[1].partition_key == 'world2'.encode('utf-8')
        
        assert b.read[2].offset == 3
        assert b.read[2].value == 'Hello3'.encode('utf-8')
        assert b.read[2].partition_key == 'world3'.encode('utf-8')
        
    @mock.patch( 'kafka_client_decorators.kafka.ConsumerFactory.get_consumer_balanced', return_value=kh )
    def test_receive_exception(self, Mockkafka ):
        self.kh.cleanMessage(  )
        self.kh.setException( KafkaException())
        self.kh.setMessage( 1, 'Hello'.encode('utf-8'), 'world'.encode('utf-8') )
        self.kh.setException( KafkaException())
        self.kh.setMessage( 2, 'Hello2'.encode('utf-8'), 'world2'.encode('utf-8') )
        self.kh.setException( KafkaException())
        self.kh.setMessage( 3, 'Hello3'.encode('utf-8'), 'world3'.encode('utf-8') )
        self.kh.setException( Exception())
        
        b = B()
        
        b.start()
        start = time.time()
        while time.time() < (start + 30):
            if len( b.read ) == 3:
                break
            time.sleep(0.01)
        
        timeout = time.time() > (start + 30)
        
        b.stop()
        b.wait()
        
        assert not timeout
        if timeout :
            return
        
        assert b.read[0].offset == 1
        assert b.read[0].value == 'Hello'.encode('utf-8')
        assert b.read[0].partition_key == 'world'.encode('utf-8')
        
        assert b.read[1].offset == 2
        assert b.read[1].value == 'Hello2'.encode('utf-8')
        assert b.read[1].partition_key == 'world2'.encode('utf-8')
        
        assert b.read[2].offset == 3
        assert b.read[2].value == 'Hello3'.encode('utf-8')
        assert b.read[2].partition_key == 'world3'.encode('utf-8')

    @mock.patch( 'kafka_client_decorators.kafka.ConsumerFactory.get_consumer_balanced', return_value=kh )
    def test_receive_stop_exception(self, Mockkafka ):
        self.kh.stopException()
        
        self.kh.cleanMessage(  )
        self.kh.setMessage( 1, 'Hello'.encode('utf-8'), 'world'.encode('utf-8') )
        
        b = B()
        
        b.start()
        b.stop()
        b.wait()
        
        assert b.read[0].offset == 1
        assert b.read[0].value == 'Hello'.encode('utf-8')
        assert b.read[0].partition_key == 'world'.encode('utf-8')
        
        self.kh.cleanMessage(  )
        self.kh.setMessage( 2, 'Hello2'.encode('utf-8'), 'world2'.encode('utf-8') )
        
        b = B()
        
        b.start()
        b.stop()
        b.wait()
        
        assert b.read[0].offset == 2
        assert b.read[0].value == 'Hello2'.encode('utf-8')
        assert b.read[0].partition_key == 'world2'.encode('utf-8')
        
if __name__ == '__main__':
    unittest.main()
