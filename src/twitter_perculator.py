import json
import logging
import doi_resolver
from event_stream.dao import DAO
import os
import sentry_sdk

from event_stream.event_stream_consumer import EventStreamConsumer
from event_stream.event_stream_producer import EventStreamProducer
from event_stream.event import Event


class TwitterPerculator(EventStreamConsumer, EventStreamProducer):
    """ link events to doi and if possible add a publication to it """
    state = "unlinked"
    group_id = "perculator"
    relation_type = "discusses"
    log = "TwitterPerculator "

    dao = None

    process_number = 3

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

        if e.get('source_id') == 'twitter':
            # logging.warning(self.log + "on message twitter perculator")

            if 'id' in e.data['subj']['data']:
                # logging.warning(self.log + e.data['subj']['data']['id'])

                # we use the id for mongo todo
                e.data['subj']['data']['_id'] = e.data['subj']['data'].pop('id')
                logging.warning(e.data['subj']['data']['_id'])
                # move matching rules to tweet self
                e.data['subj']['data']['matching_rules'] = e.data['subj']['data']['matching_rules']
                # check for doi recognition on tweet self
                doi = doi_resolver.url_doi_check(e.data['subj']['data'])
                if doi is not False:
                    self.update_event(e, doi)
                # check if we have the conversation_id in our db
                # where discussionData.subjId == conversation_id
                # check the includes object for the original tweet url
                elif 'tweets' in e.data['subj']['data']['includes']:
                    # logging.warning('tweets')
                    for tweet in e.data['subj']['data']['includes']['tweets']:
                        doi = doi_resolver.url_doi_check(tweet)
                        # logging.warning('doi 2 ' + str(doi))
                        if doi is not False:
                            # use first doi we get
                            self.update_event(e, doi)
                            break

                    if doi is False:
                        logging.warning(self.log + " no doi")
                        logging.debug(e.data['subj']['data']['includes']['tweets'])
                else:
                    logging.warning(self.log + " no doi")
                    logging.debug(e.data['subj']['data'])
            else:
                logging.warning('no id')
        else:
            logging.warning('not twitter')

    def update_event(self, event, doi):
        """update the event either with publication or just with doi and set the state accordingly
        """
        event.data['obj']['data']['doi'] = doi
        publication = self.get_publication_info(doi)

        if publication and isinstance(publication, dict) and 'title' in publication:
            self.add_publication(event, publication)
            logging.warning('linked')
            event.set('state', 'linked')
        else:
            self.add_publication(event, {'doi': doi})
            event.set('state', 'unknown')

        logging.warning(event.get('state'))
        self.publish(event)

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
    SENTRY_DSN = os.environ.get('SENTRY_DSN')
    SENTRY_TRACE_SAMPLE_RATE = os.environ.get('SENTRY_TRACE_SAMPLE_RATE')
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        traces_sample_rate=SENTRY_TRACE_SAMPLE_RATE
    )

    TwitterPerculator.start(1)