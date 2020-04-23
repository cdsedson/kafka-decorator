#!/usr/bin/python3
# -*- coding: utf-8 -*-


class ConsumerFactory:
    def get_consumer_balanced(conn, cons_args):
        kafka_client = conn.create()
        t = kafka_client.topics[cons_args.topic]
        return t.get_balanced_consumer(*cons_args.args, **cons_args.kargs)

    def get_consumer_simple(conn, cons_args):
        kafka_client = conn.create()
        t = kafka_client.topics[cons_args.topic]
        return t.get_simple_consumer(*cons_args.args, **cons_args.kargs)
