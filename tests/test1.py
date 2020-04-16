import unittest
import mock
from unittest.mock import MagicMock

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

kc = KafkaDecorator(  )

class helper_kafka:     
    def produce(self, *args, **kargs):
        if args[0] == 'Except':
            raise Exception("Error in send")
        self.message =  args[0]
        self.key = kargs['partition_key'] if 'partition_key' in kargs else None
    
    def stop(self):
        pass
          

#@kc.host(zookeeper_hosts='localhost:2181' )
@kc.host(hosts='localhost:9092' )
class A:
    def __init__(self):
        pass

    @kc.producer('test1')
    def sendKey(self, msg, key ):
        pass

#from kafka_client_decorators.kafka import ProducerFactory


class Test1(unittest.TestCase):
    kh = helper_kafka()
        
    @mock.patch( 'kafka_client_decorators.kafka.ProducerFactory.getProducer', return_value=kh )
    def teste_send_key(self, Mockkafka ):
        
        a = A()
        a.start()

        a.sendKey( 'Hello'.encode('utf-8'), partition_key='world'.encode('utf-8') )
        a.stop()
        a.wait()
        #assert 
        assert self.kh.message, keyself.kh == 'Hello'.encode('utf-8') 
        assert self.kh.key == 'world'.encode('utf-8') 
        
    @mock.patch( 'kafka_client_decorators.kafka.ProducerFactory.getProducer', return_value=kh )
    def teste_send(self, Mockkafka ):
        
        a = A()
        a.start()

        a.sendKey( 'Hello'.encode('utf-8') )
        a.stop()
        a.wait()
        #assert 
        assert self.kh.message, keyself.kh == 'Hello'.encode('utf-8') 
        assert self.kh.key == None
        
    @mock.patch( 'kafka_client_decorators.kafka.ProducerFactory.getProducer', return_value=kh )
    def teste_exception(self, Mockkafka ):
        
        a = A()
        a.start()
        
        a.sendKey( 'Except'.encode('utf-8'), partition_key='Except'.encode('utf-8') )
        a.sendKey( 'Hello'.encode('utf-8'), partition_key='world'.encode('utf-8') )
        a.stop()
        a.wait()
        #assert 
        assert self.kh.message, keyself.kh == 'Hello'.encode('utf-8') 
        assert self.kh.key == None

if __name__ == '__main__':
    unittest.main()
