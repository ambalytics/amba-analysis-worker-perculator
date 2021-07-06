import json
import logging
import doi_resolver

from event_stream_consumer import EventStreamConsumer


def url_doi_check(data):
    doi_data = False

    if 'entities' in data:
        if 'urls' in data['entities']:
            for url in data['entities']['urls']:
                if doi_data is False and 'expanded_url' in url:
                    doi_data = doi_resolver.link_url(url['expanded_url'])
                if doi_data is False and 'unwound_url' in url:
                    doi_resolver.link_url(url['unwound_url'])
            if doi_data is not False:
                return doi_data
    return doi_data


class TwitterWorker(EventStreamConsumer):
    state = "raw"
    relation_type = "discussed"

    def on_message(self, json_msg):
        print('hello')
        print(json_msg)

        worker_id = 1
        if 'id' in json_msg['data']:
            logging.warning(str(worker_id) + " " + json_msg['data']['id'])

            # we use the id for mongo
            json_msg['data']['_id'] = json_msg['data'].pop('id')
            # move matching rules to tweet self
            json_msg['data']['matching_rules'] = json_msg['matching_rules']
            running = False
            # check for doi recognition on tweet self
            doi = url_doi_check(json_msg['data'])
            logging.warning('doi 1 ' + str(doi))
            if doi is not False:
                json_msg['data']['doi'] = doi
                logging.warning(str(worker_id) + " " + json_msg['data']['_id'] + " doi self")
                # publish_message(producer, parsed_topic_name, 'parsed',
                #                 json.dumps(json_msg['data'], indent=2).encode('utf-8'))
                # check the includes object for the original tweet url
            elif 'tweets' in json_msg['includes']:
                logging.warning('tweets')
                for tweet in json_msg['includes']['tweets']:
                    doi = url_doi_check(tweet)
                    logging.warning('doi 2 ' + str(doi))
                    if doi is not False:
                        # use first doi we get
                        logging.warning(str(worker_id) + " " + json_msg['data']['_id'] + " doi includes")
                        json_msg['data']['doi'] = doi
                        # publish_message(producer, parsed_topic_name, 'parsed',
                        #                 json.dumps(json_msg['data'], indent=2).encode('utf-8'))
                        break
            else:
                logging.warning(str(worker_id) + " " + json_msg['data']['id'] + " no doi")
                # no_link.insert_one(json_msg['data'])


# from kafka import KafkaConsumer, KafkaProducer
#
# topic_name = 'events'
# global running
# running = True


# import logging
# logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)
# logging.debug('This message should go to the log file')
# logging.info('So should this')
# logging.warning('And this, too')
# logging.error('And non-ASCII stuff, too, like Øresund and Malmö')

# def publish_message(producer_instance, topic_name, key, value):
#     try:
#         key_bytes = bytes(key, encoding='utf-8')
#         value_bytes = value
#         producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
#         producer_instance.flush()
#         logging.warning('Message published successfully.')
#     except Exception as ex:
#         logging.warning('Exception in publishing message')
#         print(str(ex))
#
#
# def connect_kafka_producer():
#     _producer = None
#     try:
#         _producer = KafkaProducer(bootstrap_servers=['kafka:9092'], api_version=(0, 10))
#     except Exception as ex:
#         logging.warning('Exception while connecting Kafka')
#         logging.warning(str(ex))
#     finally:
#         return _producer
#




# def stop():
#     global running
#     running = False
#
#
# def start_worker(worker_id):
#     global running
#     running = True
#     logging.warning('Starting Consumer.. ' + str(worker_id))
#     # todo config
#     consumer = KafkaConsumer(topic_name, group_id='worker',
#                              bootstrap_servers=['kafka:9092'], api_version=(0, 10), consumer_timeout_ms=5000)
#
#     producer = connect_kafka_producer()
#     parsed_topic_name = 'linked'
#
#     while running:
#         try:
#             for msg in consumer:
#                 # print(msg.value)
#                 json_response = json.loads(msg.value)
#                 if 'id' in json_response['data']:
#                     logging.warning(str(worker_id) + " " + json_response['data']['id'])
#
#                     # we use the id for mongo
#                     json_response['data']['_id'] = json_response['data'].pop('id')
#                     # move matching rules to tweet self
#                     json_response['data']['matching_rules'] = json_response['matching_rules']
#                     running = False
#                     # check for doi recognition on tweet self
#                     doi = url_doi_check(json_response['data'])
#                     logging.warning('doi 1 ' + str(doi))
#                     if doi is not False:
#                         json_response['data']['doi'] = doi
#                         logging.warning(str(worker_id) + " " + json_response['data']['_id'] + " doi self")
#                         publish_message(producer, parsed_topic_name, 'parsed', json.dumps(json_response['data'], indent=2).encode('utf-8'))
#                         # check the includes object for the original tweet url
#                     elif 'tweets' in json_response['includes']:
#                         logging.warning('tweets')
#                         for tweet in json_response['includes']['tweets']:
#                             doi = url_doi_check(tweet)
#                             logging.warning('doi 2 ' + str(doi))
#                             if doi is not False:
#                                 # use first doi we get
#                                 logging.warning(str(worker_id) + " " + json_response['data']['_id'] + " doi includes")
#                                 json_response['data']['doi'] = doi
#                                 publish_message(producer, parsed_topic_name, 'parsed', json.dumps(json_response['data'], indent=2).encode('utf-8'))
#                                 break
#                     else:
#                         logging.warning(str(worker_id) + " " + json_response['data']['id'] + " no doi")
#                         # no_link.insert_one(json_response['data'])
#         # todo thread queue working on messages, on submit return the result and publish (async)
#
#         except Exception as exc:
#             consumer.close()
#             logging.error('%r generated an exception: %s' % (worker_id, exc))
#             logging.warning("Consumer %s closed" % (worker_id))
#             break
#
#     if running:
#         start_worker(worker_id)

# if __name__ == '__main__':
#     start_worker(1)
