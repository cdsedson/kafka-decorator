#!/usr/bin/python3
# -*- coding: utf-8 -*-

try:
    print('trying installed module')
    from kafka_client_decorators import KafkaDecorator
except:
    print('installed module failed, trying from path')
    import sys
    sys.path.insert(1, '../')
    from kafka_client_decorators import KafkaDecorator

kc = KafkaDecorator(  )

#@kc.host(zookeeper_hosts='localhost:2181' )
@kc.host(hosts='localhost:9092' )
class A:
    def __init__(self):
        print('default')
        pass

    @kc.balanced_consumer('topicPositionsJSON', consumer_group='testg1', auto_commit_enable=True, managed=True, consumer_timeout_ms=1000)
    def get(self, msg):
        print ( f'one, offset: {msg.offset} len: {len(msg.value)} ' )
        self.send2( msg.value, partition_key="chave_2".encode('utf-8') )

    @kc.simple_consumer('teste2', auto_commit_enable=True, consumer_timeout_ms=1000)
    def get2(self, msg):
        print ( f'two, offset: {msg.offset} key: {msg.partition_key} len: {len(msg.value)} ' )
        self.send( msg.value )

    @kc.producer('teste')
    def send(self, msg):
        pass

    @kc.producer('teste2')
    def send2(self, msg, key):
        pass

a = A()
import signal, os

def handler(signum, frame):
    print( 'Signal handler called with signal', signum )
    a.stop()

signal.signal(signal.SIGINT, handler)

a.start()
a.wait()
