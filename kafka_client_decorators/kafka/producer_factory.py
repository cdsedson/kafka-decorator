#!/usr/bin/python3
# -*- coding: utf-8 -*-

from pykafka import KafkaClient


class ProducerFactory:
    def get_producer(conn, prod_args):
        kafka_client = conn.create()
        t = kafka_client.topics[prod_args.topic]
        return t.get_producer(*prod_args.args, **prod_args.kargs)
