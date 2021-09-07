import json
import logging
import doi_resolver
from event_stream.dao import DAO
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from functools import lru_cache
import pymongo

from event_stream.event_stream_consumer import EventStreamConsumer
from event_stream.event_stream_producer import EventStreamProducer
from event_stream.event import Event


def get_publication_from_mongo(collection, doi):
    """get a publication from mongo db using a collection and a doi.
    this is cached up to 100

        Arguments:
            collection: the collection to use
            doi: the doi
    """
    return collection.find_one({"doi": doi})


def get_publication_from_amba(amba_client, doi):
    """get a publication from amba client using a collection and a doi.
        this is cached up to 100

        Arguments:
            amba_client: the amba_client to use
            doi: the doi
    """
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
    result = amba_client.execute(query, variable_values=params)
    if 'publicationsByDoi' in result and len(result['publicationsByDoi']) > 0:
        publication = result['publicationsByDoi'][0]
        return publication
    else:
        logging.warning('unable to get data for doi: %s' % doi)
    return None


class TwitterPerculator(EventStreamConsumer, EventStreamProducer):
    """ link events to doi and if possible add a publication to it """
    state = "unlinked"
    group_id = "perculator"
    relation_type = "discusses"
    log = "TwitterPerculator "

    amba_client = None
    dao = None

    process_number = 2

    config = {
        'url': "https://api.ambalytics.cloud/entities",
    }

    # todo --if full links in doi it must be error on confirming side?
    def on_message(self, json_msg):
        """either link a event to a publication or add doi to it and mark it unknown to add the publication finder topic

        Arguments:
            json_msg: json message representing a event
        """
        if not self.dao:
            self.dao = DAO()

        e = Event()
        e.from_json(json_msg)
        e.data['obj']['data'] = {}

        # todo check that source_id is twitter
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
                        # self.save_not_perculated(e)
            else:
                logging.warning(self.log + e.data['subj']['data']['_id'] + " no doi")
                # self.save_not_perculated(e)

    def add_publication(self, event, publication):
        """add a publication to an event

        Arguments:
            event: the event we wan't to add a publication to
            publication: the publication to add
        """
        logging.debug(self.log + "linked publication")
        event.data['obj']['data'] = publication
        doi_base_url = "https://doi.org/"  # todo
        event.data['obj']['pid'] = doi_base_url + publication['doi']
        event.data['obj']['alternative-id'] = publication['doi']
        event.set('obj_id', event.data['obj']['pid'])

    def prepare_amba_connection(self):
        """prepare the amba connection abd setup the client
        """
        transport = AIOHTTPTransport(url=self.config['url'])
        self.amba_client = Client(transport=transport, fetch_schema_from_transport=True)

    def get_publication_info(self, doi):
        """get publication data for a doi using mongo and amba dbs

        Arguments:
            doi: the doi for the publication we want
        """
        publication = self.dao.get_publication(doi)
        if publication and isinstance(publication, dict):
            logging.debug(publication)
            logging.debug('get publication from db')
            return publication

        if not self.amba_client:
            self.prepare_amba_connection()
        publication = get_publication_from_amba(self.amba_client, doi)

        if publication:
            logging.debug('get publication from amba')
            publication = self.dao.save_publication(publication)
            return publication

        return {
            'doi': doi
        }

    @staticmethod
    def start(i=0):
        """start the consumer
        """
        tp = TwitterPerculator(i)
        logging.debug(TwitterPerculator.log + 'Start %s' % str(i))
        tp.consume()


if __name__ == '__main__':
    TwitterPerculator.start(1)