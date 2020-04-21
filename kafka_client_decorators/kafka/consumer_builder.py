#!/usr/bin/python3
# -*- coding: utf-8 -*-

from .consumer_job import ConsumerJob

class ConsumerBuilder:
    SIMPLE = ConsumerJob.SIMPLE
    BALANCED = ConsumerJob.BALANCED

    def __init__(self, kind, topic, args, kargs, function):
        self.args = args
        self.kargs = kargs
        self.function = function
        self.topic = topic
        self.kind = kind

    def create(self, parent):
        return ConsumerJob(parent, self)

