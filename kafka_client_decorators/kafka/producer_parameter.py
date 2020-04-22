#!/usr/bin/python3
# -*- coding: utf-8 -*-


class ProducerParmeters:
    def __init__(self, topic, args, kargs):
        self.args = args
        self.kargs = kargs
        self.topic = topic
