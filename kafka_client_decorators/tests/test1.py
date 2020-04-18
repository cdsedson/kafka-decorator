import unittest
import mock
import time
from unittest.mock import MagicMock
from pykafka.exceptions import KafkaException
from pykafka.exceptions import ConsumerStoppedException
from ..decorators import KafkaDecorator 
from ..kafka import ConsumerFactory
from ..kafka import ProducerFactory
from ..kafka import ProducerParmeters
from ..kafka import ConnectionParmeters
from ..kafka import set_debug_level
from pykafka import KafkaClient
from threading import Lock 
import logging
 
import logging

class helper_kafka:

    def __init__(self):
        self.stop_execpt = False
        self.read = []
        self.offset = 1
        self.lock = Lock()
    
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
        with self.lock:
            self.read = []
            self.offset = 1

    def setMessageSemOffset( self, message, key ):
        with self.lock:
            self.read.append( self.M(self.offset, message, key ) )
            self.offset += 1        
       
    def setException( self, excepion  ):
        with self.lock:
            self.read.append( self.E( excepion ) )

    def produce(self, *args, **kargs):
        if args[0] == 'Except'.encode('utf-8'):
            raise Exception("Error in send")
        message =  args[0]
        key = kargs['partition_key'] if 'partition_key' in kargs else None
        self.setMessageSemOffset( message, key )
        
    def __iter__(self):
        return self
    
    def __next__(self):
        time.sleep(0.1)
        if len(self.read) <= 0:
            raise StopIteration
        message = self.read.pop(0)
        return message.get()
        
    def get_producer( self, *args, **kargs ):
        return self
        
    def get_balanced_consumer( self, *args, **kargs ):
        return self
        
    def get_simple_consumer( self, *args, **kargs ):
        return self
          

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

kc4 = KafkaDecorator(  )

@kc4.host(hosts='localhost:9092' )
class D:
    def __init__(self, testA, cls):
        self.a = testA
        self.cls = cls
        pass

    @kc4.balanced_consumer('test1', consumer_group='testgroup3', auto_commit_enable=True, managed=True, consumer_timeout_ms=1000)
    def get(self, msg):
        self.message1 = msg
        self.send( msg.value )

    @kc4.simple_consumer('test2', consumer_group='testgroup4', auto_commit_enable=True, consumer_timeout_ms=1000)
    def get2(self, msg):
        self.message2 = msg
        self.cls.stop(self)

    @kc4.producer('test2')
    def send(self, msg):
        pass

    @kc4.producer('test1')
    def sendKey(self, msg, key ):
        pass

class E:

    def __init__(self):
        pass
        
    def stop(self, conn):
        conn.stop()

class Test1(unittest.TestCase):

    topic1 = helper_kafka()
    topic2 = helper_kafka()
    topic3 = helper_kafka()
    
    @mock.patch.object( KafkaClient, '__init__', lambda self, *args, **kargs: None )
    def test_send_key(self):
        with mock.patch('pykafka.KafkaClient.topics', new_callable=mock.PropertyMock, create=True) as mock_foo:
            mock_foo.return_value = {'test1': self.topic1 }
            set_debug_level(logging.DEBUG)
            self.topic1.cleanMessage()
            
            a = A()
            a.start()

            a.sendKey( 'Hello'.encode('utf-8'), partition_key='world'.encode('utf-8') )
            
            time.sleep(0.5)
            a.stop()
            a.wait()

            assert self.topic1.read[0].offset == 1 
            assert self.topic1.read[0].value == 'Hello'.encode('utf-8') 
            assert self.topic1.read[0].partition_key == 'world'.encode('utf-8')
        
    @mock.patch.object( KafkaClient, '__init__', lambda self, *args, **kargs: None )
    def test_start_stop(self):
        with mock.patch('pykafka.KafkaClient.topics', new_callable=mock.PropertyMock, create=True) as mock_foo:
            mock_foo.return_value = {'test1': self.topic1 }
            self.topic1.cleanMessage()
        
            a = A()
            a.start()

            a.sendKey( 'Hello'.encode('utf-8'), partition_key='world'.encode('utf-8') )
            assert not a.is_fineshed()
            time.sleep(0.5)
            assert not a.is_fineshed()
            a.sendKey( 'Hello2'.encode('utf-8'), partition_key='world2'.encode('utf-8') )
            assert not a.is_fineshed()
            a.stop()
            a.wait()
            assert a.is_fineshed()
            assert self.topic1.read[0].offset == 1 
            assert self.topic1.read[0].value == 'Hello'.encode('utf-8')
            assert self.topic1.read[0].partition_key == 'world'.encode('utf-8')
            assert self.topic1.read[1].offset == 2 
            assert self.topic1.read[1].value == 'Hello2'.encode('utf-8') 
            assert self.topic1.read[1].partition_key == 'world2'.encode('utf-8') 
    
    @mock.patch.object( KafkaClient, '__init__', lambda self, *args, **kargs: None )
    def test_send(self ):
        with mock.patch('pykafka.KafkaClient.topics', new_callable=mock.PropertyMock, create=True) as mock_foo:
            mock_foo.return_value = {'test1': self.topic1 }
            self.topic1.cleanMessage()
            
            a = A()
            a.start()

            a.sendKey( 'Hello'.encode('utf-8') )
            a.stop()
            a.wait()
            
            assert self.topic1.read[0].offset == 1 
            assert self.topic1.read[0].value == 'Hello'.encode('utf-8') 
            assert self.topic1.read[0].partition_key is None 
        
    @mock.patch.object( KafkaClient, '__init__', lambda self, *args, **kargs: None )
    def test_exception(self ):
        with mock.patch('pykafka.KafkaClient.topics', new_callable=mock.PropertyMock, create=True) as mock_foo:
            mock_foo.return_value = {'test1': self.topic1 }
            self.topic1.cleanMessage()

            a = A()
            a.start()

            a.sendKey( 'Except'.encode('utf-8'), partition_key='Except'.encode('utf-8') )

            assert len(self.topic1.read) == 0 
            
            a.sendKey( 'Hello'.encode('utf-8'), partition_key='world'.encode('utf-8') )
            a.stop()
            a.wait()

            assert self.topic1.read[0].offset == 1 
            assert self.topic1.read[0].value == 'Hello'.encode('utf-8') 
            assert self.topic1.read[0].partition_key  == 'world'.encode('utf-8')

        
    @mock.patch.object( KafkaClient, '__init__', lambda self, *args, **kargs: None )
    def test_stop_exception(self ):
        with mock.patch('pykafka.KafkaClient.topics', new_callable=mock.PropertyMock, create=True) as mock_foo:
            mock_foo.return_value = {'test1': self.topic1 }
            self.topic1.cleanMessage()
            
            self.topic1.stopException()
        
            a = A()
            a.start()
        
            a.sendKey( 'Erro'.encode('utf-8'), partition_key='Erro'.encode('utf-8') )            
            a.stop()
            a.wait()
            
            assert self.topic1.stop_execpt is False
            
            a = A()
            a.start()
            
            a.sendKey( 'Hello'.encode('utf-8'), partition_key='world'.encode('utf-8') )
            
            a.stop()
            a.wait()
            assert self.topic1.read[0].offset == 1 
            assert self.topic1.read[0].value == 'Erro'.encode('utf-8') 
            assert self.topic1.read[0].partition_key  == 'Erro'.encode('utf-8')
            assert self.topic1.read[1].offset == 2 
            assert self.topic1.read[1].value == 'Hello'.encode('utf-8') 
            assert self.topic1.read[1].partition_key  == 'world'.encode('utf-8')
            
            assert self.topic1.stop_execpt is False
        
    @mock.patch.object( KafkaClient, '__init__', lambda self, *args, **kargs: None )
    def test_receive(self ):
        with mock.patch('pykafka.KafkaClient.topics', new_callable=mock.PropertyMock, create=True) as mock_foo:
            mock_foo.return_value = {'test1': self.topic1 }
            self.topic1.cleanMessage()
            
            self.topic1.setMessageSemOffset( 'Hello'.encode('utf-8'), 'world'.encode('utf-8') )
        
            b = B()
            
            b.start()
            b.stop()
            b.wait()

            assert b.read[0].offset == 1
            assert b.read[0].value == 'Hello'.encode('utf-8')
            assert b.read[0].partition_key == 'world'.encode('utf-8')
        
    @mock.patch.object( KafkaClient, '__init__', lambda self, *args, **kargs: None )
    def test_receive_simple( self ):
        with mock.patch('pykafka.KafkaClient.topics', new_callable=mock.PropertyMock, create=True) as mock_foo:
            mock_foo.return_value = {'test1': self.topic1 }
            self.topic1.cleanMessage()
      
            self.topic1.setMessageSemOffset( 'Hello'.encode('utf-8'), 'world'.encode('utf-8') )
            
            c = C()
            
            c.start()
            c.stop()
            c.wait()
            
            assert c.read[0].offset == 1
            assert c.read[0].value == 'Hello'.encode('utf-8')
            assert c.read[0].partition_key == 'world'.encode('utf-8')
            
    @mock.patch.object( KafkaClient, '__init__', lambda self, *args, **kargs: None )    
    def test_receive_exception(self ):
        with mock.patch('pykafka.KafkaClient.topics', new_callable=mock.PropertyMock, create=True) as mock_foo:
            mock_foo.return_value = {'test1': self.topic1 }
            self.topic1.cleanMessage()
        
            self.topic1.setMessageSemOffset( 'Hello'.encode('utf-8'), 'world'.encode('utf-8') )
            self.topic1.setMessageSemOffset( 'Hello2'.encode('utf-8'), 'world2'.encode('utf-8') )
            self.topic1.setException( ConsumerStoppedException()  )
            self.topic1.setMessageSemOffset( 'Hello3'.encode('utf-8'), 'world3'.encode('utf-8') )
            
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
        
    @mock.patch.object( KafkaClient, '__init__', lambda self, *args, **kargs: None )  
    def test_receive_exception(self ):
        with mock.patch('pykafka.KafkaClient.topics', new_callable=mock.PropertyMock, create=True) as mock_foo:
            mock_foo.return_value = {'test1': self.topic1 }
            self.topic1.cleanMessage()
            
            self.topic1.setException( KafkaException())
            self.topic1.setMessageSemOffset( 'Hello'.encode('utf-8'), 'world'.encode('utf-8') )
            self.topic1.setException( KafkaException())
            self.topic1.setMessageSemOffset( 'Hello2'.encode('utf-8'), 'world2'.encode('utf-8') )
            self.topic1.setException( KafkaException())
            self.topic1.setMessageSemOffset( 'Hello3'.encode('utf-8'), 'world3'.encode('utf-8') )
            self.topic1.setException( Exception())
            
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

    @mock.patch.object( KafkaClient, '__init__', lambda self, *args, **kargs: None )  
    def test_receive_stop_exception(self):
        with mock.patch('pykafka.KafkaClient.topics', new_callable=mock.PropertyMock, create=True) as mock_foo:
            mock_foo.return_value = {'test1': self.topic1 }
            self.topic1.cleanMessage()
            
            self.topic1.stopException()
            self.topic1.setMessageSemOffset( 'Hello'.encode('utf-8'), 'world'.encode('utf-8') )
            
            b = B()
            
            b.start()
            b.stop()
            b.wait()
            
            assert b.read[0].offset == 1
            assert b.read[0].value == 'Hello'.encode('utf-8')
            assert b.read[0].partition_key == 'world'.encode('utf-8')
            
            self.topic1.setMessageSemOffset( 'Hello2'.encode('utf-8'), 'world2'.encode('utf-8') )
            
            b = B()
            
            b.start()
            b.stop()
            b.wait()
            
            assert b.read[0].offset == 2
            assert b.read[0].value == 'Hello2'.encode('utf-8')
            assert b.read[0].partition_key == 'world2'.encode('utf-8')

    @mock.patch.object( KafkaClient, '__init__', lambda self, *args, **kargs: None )  
    def test_send_receive_many(self):
        with mock.patch('pykafka.KafkaClient.topics', new_callable=mock.PropertyMock, create=True) as mock_foo:
            mock_foo.return_value = {'test1': self.topic2,  'test2': self.topic3}
            self.topic1.cleanMessage()
            self.topic2.cleanMessage()

            a = D('Example', E())
            a.start()
            a.sendKey( 'Hello'.encode('utf-8'), partition_key='world'.encode('utf-8') )
            a.wait() 
            
            assert a.message1.offset == 1
            assert a.message1.value == 'Hello'.encode('utf-8')
            assert a.message1.partition_key == 'world'.encode('utf-8')
            
            assert a.message1.offset == 1
            assert a.message1.value == 'Hello'.encode('utf-8')
            assert a.message1.partition_key == 'world'.encode('utf-8')
            
if __name__ == '__main__':
    unittest.main()
