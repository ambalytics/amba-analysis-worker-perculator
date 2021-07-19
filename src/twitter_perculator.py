import json
import logging
import doi_resolver
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

from event_stream.event_stream_consumer import EventStreamConsumer
from event_stream.event_stream_producer import EventStreamProducer
from event_stream.event import Event


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
    state = "unlinked"
    group_id = "perculator"
    relation_type = "discusses"
    publication_client = False
    log = "TwitterPerculator "
    cached_publications = {}

    def on_message(self, json_msg):
        e = Event()
        e.from_json(json_msg)
        e.data['obj']['data'] = {}
        # todo add mongo, cache
        # json_msg = e.data['subj']['data']
        # logging.warning(self.log + "on message twitter perculator")

        if 'id' in e.data['subj']['data']:
            logging.warning(self.log + e.data['subj']['data']['id'])

            # we use the id for mongo
            e.data['subj']['data']['_id'] = e.data['subj']['data'].pop('id')
            # move matching rules to tweet self
            e.data['subj']['data']['matching_rules'] = e.data['subj']['data']['matching_rules']
            running = False
            # check for doi recognition on tweet self
            doi = url_doi_check(e.data['subj']['data'])
            # logging.warning('doi 1 ' + str(doi))
            if doi is not False:
                e.data['obj']['data']['doi'] = doi
                publication = self.get_publication_info(doi)
                if publication is not False:
                    self.add_publication(e, publication)
                    logging.warning('publish linked message of doi ')
                else:
                    e.set('state', 'unknown')
                self.publish(e)
                # check the includes object for the original tweet url
            elif 'tweets' in e.data['subj']['data']['includes']:
                # logging.warning('tweets')
                for tweet in e.data['subj']['data']['includes']['tweets']:
                    doi = url_doi_check(tweet)
                    # logging.warning('doi 2 ' + str(doi))
                    if doi is not False:
                        # use first doi we get
                        # logging.warning(self.log + e.data['subj']['data']['_id'] + " doi includes")
                        e.data['obj']['data']['doi'] = doi  # todo why are full links in doi
                        publication = self.get_publication_info(doi)
                        if publication is not False:
                            self.add_publication(e, publication)
                            logging.warning('publish linked message of doi in includes')
                        else:
                            e.set('state', 'unknown')
                        self.publish(e)
                        # publish_message(producer, parsed_topic_name, 'parsed',
                        #                 json.dumps(json_msg['data'], indent=2).encode('utf-8'))
                        break
            else:
                logging.warning(self.log + e.data['subj']['data']['_id'] + " no doi")
                # todo put into unknown

    def add_publication(self, event, publication):
        logging.warning(self.log + "linked publication")
        event.data['obj']['data'] = publication
        # event.data['obj']['pid'] = publication['id']
        # event.set('obj_id', )
        event.set('state', 'linked')

    def prepare_publication_connection(self):
        url = "https://api.ambalytics.cloud/entities"
        transport = AIOHTTPTransport(url=url)
        self.publication_client = Client(transport=transport, fetch_schema_from_transport=True)

    def get_publication_info(self, doi):
        # todo cache
        # todo add mongo
        publication = self.cached_publications.get(doi)
        if publication:
            logging.debug('get publication from cache')
            return publication

        if not self.publication_client:
            self.prepare_publication_connection()

        query = gql(
            """
            query getPublication($doi: [String!]!) {
             publicationsByDoi(doi: $doi) {
              id,
              type,
              doi,
              abstract,
              pubDate,
              publisher,
              rank,
              citationCount,
              title,
              normalizedTitle,
              year,
              citations {
                  id,
                  type,
                  doi,
                  abstract,
                  pubDate,
                  publisher,
                  rank,
                  citationCount,
                  title,
                  normalizedTitle,
                  year
              },
              refs  {
                  id,
                  type,
                  doi,
                  abstract,
                  pubDate,
                  publisher,
                  rank,
                  citationCount,
                  title,
                  normalizedTitle,
                  year
              },
              authors {
                  id,
                  name,
                  normalizedName,
                  pubCount,
                  citationCount,
                  rank
              },
              fieldsOfStudy {
                  score,
                  name,
                  normalizedName,
                  level,
                  rank,
                  citationCount
              }
            } 
        }

        """)

        # todo  affiliation: Affiliation author
        #   parents: [FieldOfStudy!]
        #   children: [FieldOfStudy!]

        params = {"doi": doi}
        result = self.publication_client.execute(query, variable_values=params)
        if 'publicationsByDoi' in result and len(result['publicationsByDoi']) > 0:
            # todo better way?
            publication = result['publicationsByDoi'][0]
            self.cached_publications[doi] = publication
            return publication
        else:
            logging.warning(self.log + 'unable to get data for doi: %s' % doi)
        return False
