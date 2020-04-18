#!/usr/bin/python
# -*- coding: <encoding name> -*-

from pykafka import KafkaClient

class ConsumerFactory:
    def get_consumer_balanced( conn_args, cons_args  ):
        kafka_client = KafkaClient( *conn_args.args, **conn_args.kargs )
        t = kafka_client.topics[cons_args.topic]
        return t.get_balanced_consumer( *cons_args.args, **cons_args.kargs )
            
    def get_consumer_simple( conn_args, cons_args ):
        kafka_client = KafkaClient( *conn_args.args, **conn_args.kargs )
        t = kafka_client.topics[cons_args.topic]
        return t.get_simple_consumer( *cons_args.args, **cons_args.kargs )
        
	
