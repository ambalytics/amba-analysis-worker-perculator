import json
import logging

from .event_stream_base import EventStreamBase
from kafka import KafkaConsumer, KafkaProducer

from multiprocessing import Process, Queue, current_process, freeze_support


# idee
# import eventstream reader
# class inherince
# override for each message method, use var as string?
#     -> goal of eventstream
#
# o1 here is function, do everything else (mulitple threads etc)
#
# o2 here is a class you can ran as you wish
#
# o3 (1+2) eventstream class has functions to do multithreads with the class
#
# consumer
# producer
# consumer producer
#
# -> event stream problem (handle multiple or just one each?)
# eventstreap processor process producer1, consumer2,

class EventStreamConsumer(EventStreamBase):

    relation_type: str = ''
    state: str = "raw"
    task_queue = Queue()
    process_number = 4
    log = "EventStreamConsumer " + str(id) + " "

    def get_consumer(self):
        if not self.topic_name:
            self.topic_name = self.build_topic_name(state=self.state, relation_type=self.relation_type)
        self.topic_name = 'tweets'
        logging.debug("get consumer for topic: %s" % self.topic_name)
        return KafkaConsumer(self.topic_name, group_id=self.group_id,
                             bootstrap_servers=[self.bootstrap_servers], api_version=self.api_version, consumer_timeout_ms=self.consumer_timeout_ms)


    def consume(self):
        self.running = True

        consumer = self.get_consumer()

        # Start worker processes
        for i in range(self.process_number):
            Process(target=self.on_message, args=(self.task_queue,)).start()

        while self.running:
            try:
                for msg in consumer:
                    logging.warning(msg)
                    self.task_queue.put(json.loads(msg.value))

            except Exception as exc:
                consumer.close()
                logging.error('%r generated an exception: %s' % exc)
                logging.warning("Consumer %s closed")
                break

        if self.running:
            self.consume()

    def on_message(self, json_msg):
        print(json_msg)

    def stop(self):
        self.running = False
        print('stop running consumer')

if __name__ == '__main__':
    e = EventStreamConsumer()
    print(e.consume())
