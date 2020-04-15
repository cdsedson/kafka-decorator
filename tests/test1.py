import unittest


#class TestSum(unittest.TestCase):

#    def test_sum(self):
#        self.assertEqual(sum([1, 2, 3]), 6, "Should be 6")

#    def test_sum_tuple(self):
#        self.assertEqual(sum((1, 2, 2)), 6, "Should be 6")



#!/usr/bin/python
# -*- coding: <encoding name> -*-

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

#@kc.host(zookeeper_hosts='localhost:2181' )
@kc.host(hosts='localhost:9092' )
class A:
    def __init__(self):
        pass

    @kc.producer('test1')
    def sendKey(self, msg, key ):
        pass

class TestSum(unittest.TestCase):
    def teste_Send(self):
        a = A()
        a.start()

        a.sendKey( 'Hello'.encode('utf-8'), partition_key='world'.encode('utf-8') )
        a.stop()
        a.wait()


if __name__ == '__main__':
    unittest.main()
