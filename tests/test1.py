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
        print( args, kargs)
        self.m =  (args, kargs)
    
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
    @mock.patch( 'kafka_client_decorators.kafka.ProducerFactory.getProducer', return_value=helper_kafka() )
    def teste_Send(self, Mockkafka ):
        
        #Mockkafka.getProducer = MagicMock(return_value=helper_kafka())
        #attrs = {'topic': 3, 'other.side_effect': KeyError}
        #Mockkafka.configure_mock(**attrs)
        #print(Mockkafka)
        #Mockkafka.return_value = helper_kafka
        #Mockkafka.topic  = [helper_kafka()] 
        #print(dir(Mockkafka))
        a = A()
        a.start()

        a.sendKey( 'Hello'.encode('utf-8'), partition_key='world'.encode('utf-8') )
        a.stop()
        a.wait()
        #assert 
        print( Mockkafka.call_args_list)


if __name__ == '__main__':
    unittest.main()
