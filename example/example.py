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
    def __init__(self, testA, cls):
        self.a = testA
        self.cls = cls
        pass

    @kc.balanced_consumer('test1', consumer_group='testgroup3', auto_commit_enable=True, managed=True, consumer_timeout_ms=1000)
    def get(self, msg):
        print ( f'{self.a} Receive offset {msg.offset} key {msg.partition_key} message: { msg.value }' )
        self.send( msg.value )

    @kc.simple_consumer('test2', consumer_group='testgroup4', auto_commit_enable=True, consumer_timeout_ms=1000)
    def get2(self, msg):
        print ( f'{self.a} Receive offset {msg.offset}, message: { msg.value }' )
        self.cls.stop(self)

    @kc.producer('test2')
    def send(self, msg):
        pass

    @kc.producer('test1')
    def sendKey(self, msg, key ):
        pass

class B:

    def __init__(self):
        pass
        
    def stop(self, conn):
        conn.stop()

a = A('Example', B())
a.start()

a.sendKey( 'Hello'.encode('utf-8'), partition_key='world'.encode('utf-8') )

a.wait()



