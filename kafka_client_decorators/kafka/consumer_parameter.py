#!/usr/bin/python
# -*- coding: <encoding name> -*-

class ConsumerParmeters:
    SIMPLE = 1
    BALANCED = 2
    def __init__( self, kind, topic, args, kargs, function ):
        self.args = args
        self.kargs = kargs
        self.function = function
        self.topic = topic
        self.kind = kind