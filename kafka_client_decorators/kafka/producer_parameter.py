#!/usr/bin/python
# -*- coding: <encoding name> -*-

class ProducerParmeters:
    def __init__( self, topic, args, kargs ):
        self.args = args
        self.kargs = kargs
        self.topic = topic
        