#!/usr/bin/python3
# -*- coding: utf-8 -*-

from .producer import Producer

class ProducerBuilder:
    def __init__(self, name, topic, args, kargs):
        self.name = name
        self.args = args
        self.kargs = kargs
        self.topic = topic

    def create(self, parent):
        return Producer(parent, self)
