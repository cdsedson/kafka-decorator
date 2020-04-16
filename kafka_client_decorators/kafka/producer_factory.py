#!/usr/bin/python
# -*- coding: <encoding name> -*-

from pykafka import KafkaClient

class ProducerFactory:
    def getProducer( conn_args, prod_args, topic  ):
         kafka_client = KafkaClient( *conn_args.args, **conn_args.kargs)
         t = kafka_client.topics[topic] 
         return t.get_producer( *prod_args.args, **prod_args.kargs )
        
	