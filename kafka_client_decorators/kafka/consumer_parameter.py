class ConsumerParmeters:
    def __init__( self, topic, args, kargs, function ):
        self.args = args
        self.kargs = kargs
        self.function = function
        self.topic = topic