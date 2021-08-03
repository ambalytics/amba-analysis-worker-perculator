import json
import logging
import doi_resolver
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from functools import lru_cache
import pymongo

from event_stream.event_stream_consumer import EventStreamConsumer
from event_stream.event_stream_producer import EventStreamProducer
from event_stream.event import Event


class TwitterPerculator(EventStreamConsumer, EventStreamProducer):
    state = "unlinked"
    group_id = "perculator"
    relation_type = "discusses"
    log = "TwitterPerculator "

    amba_client = None
    mongo_client = None

    config = {
        'mongo_url': "mongodb://mongo_db:27017/",
        'mongo_client': "events",
        'mongo_collection': "publication",
        'url': "https://api.ambalytics.cloud/entities",
    }

    # todo if full links in doi it must be error on confirming side?
    def on_message(self, json_msg):
        e = Event()
        e.from_json(json_msg)
        e.data['obj']['data'] = {}

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
            doi = doi_resolver.url_doi_check(e.data['subj']['data'])
            # logging.warning('doi 1 ' + str(doi))
            if doi is not False:
                e.data['obj']['data']['doi'] = doi
                # e.data['doiTemp'] = doi
                publication = self.get_publication_info(doi)
                self.add_publication(e, publication)

                if 'title' in publication:
                    e.set('state', 'linked')
                    logging.warning('publish linked message of doi ')
                else:
                    e.set('state', 'unknown')

                self.publish(e)
                # check the includes object for the original tweet url
            elif 'tweets' in e.data['subj']['data']['includes']:
                # logging.warning('tweets')
                for tweet in e.data['subj']['data']['includes']['tweets']:
                    doi = doi_resolver.url_doi_check(tweet)
                    # logging.warning('doi 2 ' + str(doi))
                    if doi is not False:
                        # use first doi we get
                        # logging.warning(self.log + e.data['subj']['data']['_id'] + " doi includes")
                        e.data['obj']['data']['doi'] = doi
                        # e.data['doiTemp'] = doi
                        publication = self.get_publication_info(doi)
                        self.add_publication(e, publication)

                        if 'title' in publication:
                            e.set('state', 'linked')
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
        doi_base_url = "https://doi.org/"  # todo
        event.data['obj']['pid'] = doi_base_url + publication['doi']
        event.data['obj']['alternative-id'] = publication['doi']
        event.set('obj_id', event.data['obj']['pid'])

    def prepare_amba_connection(self):
        transport = AIOHTTPTransport(url=self.config['url'])
        self.amba_client = Client(transport=transport, fetch_schema_from_transport=True)

    def prepare_mongo_connection(self):
        self.mongo_client = pymongo.MongoClient(host=self.config['mongo_url'],
                                               serverSelectionTimeoutMS=3000,  # 3 second timeout
                                               username="root",
                                               password="example"
                                               )
        self.db = self.mongo_client[self.config['mongo_client']]
        self.collection = self.db[self.config['mongo_collection']]

    @lru_cache(maxsize=1000)
    def get_publication_info(self, doi):
        publication = self.get_publication_from_mongo(doi)
        if publication:
            logging.debug('get publication from mongo')
            return publication

        publication = self.get_publication_from_amba(doi)
        if publication:
            logging.debug('get publication from amba')
            self.save_publication_to_mongo(publication)
            return publication

        return {
            'doi': doi
        }

    def get_publication_from_mongo(self, doi):
        if not self.mongo_client:
            self.prepare_mongo_connection()
        return self.collection.find_one({"doi": doi})

    # save the publication to our mongo to
    # this allows faster access and to store calculated data right on it
    # additional ist easier only to check one db
    def save_publication_to_mongo(self, publication):
        logging.debug('save publication to mongo')
        try:
            # publication['_id'] = publication['id']
            publication['_id'] = publication['doi']
            publication['source'] = 'amba'
            publication['source-a'] = 'perculator' # todo remove
            self.collection.insert_one(publication)
        except pymongo.errors.DuplicateKeyError:
            logging.warning("MongoDB publication, Duplicate found, continue" % publication)

    def get_publication_from_amba(self, doi):
        if not self.amba_client:
            self.prepare_amba_connection()

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
        result = self.amba_client.execute(query, variable_values=params)
        if 'publicationsByDoi' in result and len(result['publicationsByDoi']) > 0:
            # todo better way?
            publication = result['publicationsByDoi'][0]
            return publication
        else:
            logging.warning(self.log + 'unable to get data for doi: %s' % doi)
        return None
