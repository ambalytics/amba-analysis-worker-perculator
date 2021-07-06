import json
import logging
import doi_resolver
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

from EventStream.event_stream_consumer import EventStreamConsumer
from EventStream.event_stream_producer import EventStreamProducer
from EventStream.event import Event


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


class TwitterPerculator(EventStreamConsumer, EventStreamProducer):
    state = "raw"
    group_id = "perculator"
    relation_type = "discusses"
    publication_client = False
    log = "TwitterPerculator "

    def on_message(self, json_msg):
        e = Event()
        e.from_json(json_msg)

        # todo
        json_msg = e.data['subj']['data']
        logging.warning(self.log + "on message twitter perculator")

        if 'id' in json_msg['data']:
            logging.warning(self.log + json_msg['data']['id'])

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
                logging.warning(self.log + json_msg['data']['_id'] + " doi self")
                e.data['subj']['data'] = json_msg
                e.set('state', 'linked')
                self.publish(e)
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
                        logging.warning(self.log + json_msg['data']['_id'] + " doi includes")
                        json_msg['data']['doi'] = doi
                        publication = self.get_publication_info(doi)
                        if not publication:
                            json_msg['publication'] = publication
                            logging.warning(self.log + "linked publication")
                            e.data['subj']['data'] = json_msg
                            e.set('state', 'linked')
                            self.publish(e)
                            # publish_message(producer, parsed_topic_name, 'parsed',
                            #                 json.dumps(json_msg['data'], indent=2).encode('utf-8'))
                        break
            else:
                logging.warning(self.log + json_msg['data']['_id'] + " no doi")
                # no_link.insert_one(json_msg['data'])

    def prepare_publication_connection(self):
        url = "https://api.ambalytics.cloud/entities"
        transport = AIOHTTPTransport(url=url)
        self.publication_client = Client(transport=transport, fetch_schema_from_transport=True)

    def get_publication_info(self, doi):
        # todo cache

        if not self.publication_client:
            self.prepare_publication_connection()

        query = gql(
            """
            query getPublication($doi: [String!]!) {
             publicationsByDoi(doi: $doi) {

              doi,
              abstract,
              pubDate,
              publisher,
              rank,
              citationCount,
              title,
              normalizedTitle,
              year,

            } 
        }

        """)

        params = {"doi": doi}
        result = self.publication_client.execute(query, variable_values=params)
        if 'publicationsByDoi' in result and len(result['publicationsByDoi']) > 0:
            # todo better way?
            return result['publicationsByDoi'][0]
        else:
            logging.warning(self.log + 'unable to get data for doi: %s' % doi)
        return False
