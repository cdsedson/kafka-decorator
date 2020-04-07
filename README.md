

This module is based on pykafka project (https://github.com/Parsely/pykafka.git) 
and define a class KafkaDecorator and its 4 decorators

@KafkaDecorator.host
The first defines the connection parameters
its parameters are the same of pykafka.KafkaClient  


@KafkaDecorator.balanced_consumer
its parameters are the same of pykafka.topic.get_balanced_consumer function


@KafkaDecorator.simple_consumer
its parameters are the same of pykafka.topic.get_simple_consumer function

its parameters are the same of pykafka.topic.get_producer function
@KafkaDecorator.producer

Install

	pip install kafka-client-decorators
 
How to use

	from kafka_client_decorators import KafkaDecorator
	from kafka_client_decorators.kafka.logging_helper import setDebugLevel
	import logging


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



