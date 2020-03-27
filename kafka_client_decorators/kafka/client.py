from .consumer_job import ConsumerJob 
from .producer import Producer

from threading import Thread

class Client(Thread):
    def __init__(self, *args, **kargs ):
        self.__started__  = True
        Thread.__init__(self)

    def __createConsumers__( self ):
        conargs, conkargs = self.decor.kafka_args
        consumers = []
        for topic, consumer in self.decor.__list_topics__.items():
            args, kargs, f = consumer
            try:
                job = ConsumerJob( self, conargs, conkargs, topic, args, kargs, f )
                consumers.append( job )
            except Exception as e:
                print(e)
                consumers = []
                break
            except:
                print("Erro desconhecido")
                consumers = []
                break
        return consumers
        
    def __startConsumers__( self, consumers ):
        for c in consumers:
            c.start()
    
    def __stopConsumer__( self, consumers ):
        for st in consumers:
            st.stop()
            
    def __waitConsumer__( self, consumers ):
        if self.__started__  is False:
            self.__stopConsumer__( consumers )
        
        init = False
        for t in consumers:
            if t.is_alive() is True: 
                t.join(0.1)
            if t.is_alive() is True:
                init = True
            else:
                self.__started__  = False
        return init
                        
    def __waitConsumersFinish__( self, consumers ):
        init = True
        while init is True:
            init = self.__waitConsumer__( consumers )
            
    def __waitProducersFinish__( self ):
        init = True
        while self.__started__  is True:
            time.sleep(0.01)
        for p in self.decor.__list_topics_send__.values():
            if p[2] is not None:
                p[2].stop()
                          
    def run(self):
        self.__started__  = True
        
        while self.__started__  == True:
            try:
                conargs, conkargs = self.decor.kafka_args
                consumers = self.__createConsumers__( )
                self.__startConsumers__( consumers )
                self.__waitConsumersFinish__( consumers )
                self.__waitProducersFinish__( )
            except Exception as e:
                print(e)
            except:
                print("Erro desconhecido")

    def producer(self, topic, *func_args, **func_kargs ):
        args, kargs, p = self.decor.__list_topics_send__[topic]
        success = True
        try:
            if p is None:
                conargs, conkargs = self.decor.kafka_args
                p = Producer( conargs, conkargs, topic, args, kargs )
                self.decor.__list_topics_send__[topic] = (args, kargs, p)
                
            p.produce( *func_args, **func_kargs )
        except (Exception) as e:
            print(e)
            success = False
        except:
            print("Erro desconhecido")
            success = False
        return success

    def stop(self):
        self.__started__  = False
        
    def wait(self):
        while self.is_alive( ) is True:
            self.join( 0.01 )
        
    def is_fineshed(self):
        return self.is_alive( ) != True
