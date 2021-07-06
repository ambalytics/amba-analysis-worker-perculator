import logging

#from kafka import KafkaConsumer, KafkaProducer


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

class EventStreamBase(object):

    event_string: str = "events"
    state_separator: str = "_"
    relation_type_separator: str = "-"

    bootstrap_servers = ['kafka:9092']
    group_id = 'worker'
    topic_name = False
    consumer_timeout_ms = 5000
    api_version = (0, 10)

    running = False

    config_states = {
        'raw': {
            'own_topic': ['discussed', 'crossref']
        },
        'linked': {
            'own_topic': ['discussed']
        },
        'unknown': {
        },
        'processed': {
            'own_topic': ['discussed']
        },
        'aggregated': {
        }}

    topics = []

    def build_topic_list(self):
        result = []

        for c_state in self.config_states:
            result.append(self.build_topic_name(c_state))
            # print(c_state)
            # print(self.config_states[c_state])
            if 'own_topic' in self.config_states[c_state]:
                for c_o_topic in self.config_states[c_state]['own_topic']:
                    result.append(self.build_topic_name(c_state, c_o_topic))

        self.topics = result
        logging.warning("current topics for events: %s" % self.topics)
        return result

    def build_topic_name(self, state, relation_type=''):
        result = self.event_string + self.state_separator + state

        if relation_type != '':
            result = result + self.relation_type_separator + relation_type
        return result

    # def load_config(self):
    #     #data = yaml.safe_load(open('defaults.yaml'))
    #     #data['url']

    def resolveEvent(self, event):
        topic_name = self.build_topic_name(event['state'], event['relation_type'])
        if topic_name in self.topics:
            return topic_name

        logging.warning("Unable to resolve event, topic_name %s not found" % topic_name)
        return False


if __name__ == '__main__':
    e = EventStreamBase()
    print(e.build_topic_list())
