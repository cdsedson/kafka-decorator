#!/usr/bin/python3
# -*- coding: utf-8 -*-

from pykafka import KafkaClient

from .consumer_factory import ConsumerFactory
from .producer_factory import ProducerFactory


class ConnectionBuilder:
    def __init__(self, args, kargs):
        self.__args__ = args
        self.__kargs__ = kargs

    def create_topic(self, topic):
        kafka_client = KafkaClient(*self.__args__, **self.__kargs__)
        return kafka_client.topics[topic]

    def get_consumer(self, topic, balanced, args, kargs):
        return ConsumerFactory(self, topic, balanced, args, kargs)

    def get_producer(self, topic, args, kargs):
        return ProducerFactory(self, topic, args, kargs)
