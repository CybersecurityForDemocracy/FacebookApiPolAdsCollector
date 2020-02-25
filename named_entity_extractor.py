""" Wrapper for all GCP Language API code.

You can read more about it here: https://cloud.google.com/natural-language/docs/basics
You can generate a new credentials file here: https://console.cloud.google.com/apis/credentials/serviceaccountkey?project=nyupoladstransparency&folder=&organizationId=&angularJsUrl=%2Fapis%2Fcredentials%2Fserviceaccountkey%3Fsupportedpurview%3Dproject%26project%3Dnyupoladstransparency%26folder%3D%26organizationId%3D&supportedpurview=project
"""
import logging
import sys
import json
from collections import defaultdict

from google.cloud import language_v1
from google.cloud.language_v1 import enums
from google.protobuf.json_format import MessageToDict

import db_functions
import generic_fb_collector

GCS_CREDENTIALS_FILE = 'credentials.json'
ENTITY_MAP_FILE = 'map_for_date.json'


class NamedEntityAnalysis(object):
    """ This Class Handles All aspects of the Name Entity Recognition task.

    No GCP Language API specific datastructures/code should escape this class.
    """

    def __init__(self, credentials_file=GCS_CREDENTIALS_FILE):
        self.client = language_v1.LanguageServiceClient.from_service_account_json(credentials_file)

    def _store_all_results(self, cluster_id, ner_analysis_result):
        """ Store complete result for deeper analysis as needed later. """
        # TODO: Implement this
        logging.error(
            '_store_all_results is not implemented! printing to terminal instead:\n%s', ner_analysis_result)

    def _load_all_results(self, cluster_id):
        """ Store complete result for deeper analysis as needed later. """
        # TODO: Implement this
        dummy_response = {'entities': [
            {'name': 'Kermit', 'type': 'PERSON', 'metadata': {'wikipedia_url': 'https://en.wikipedia.org/wiki/Kermit_the_Frog', 'mid': '/m/04bsc'}, 'salience': 0.5452021360397339, 'mentions': [{'text': {'content': 'Kermit'}, 'type': 'PROPER'}]},
            {'name': 'President', 'type': 'PERSON', 'salience': 0.24969187378883362, 'mentions': [{'text': {'content': 'President', 'beginOffset': 24}, 'type': 'COMMON'}]},
            {'name': 'Tim Curry', 'type': 'PERSON', 'metadata': {'wikipedia_url': 'https://en.wikipedia.org/wiki/Tim_Curry', 'mid': '/m/07rzf'}, 'salience': 0.1645415723323822, 'mentions': [{'text': {'content': 'Tim Curry', 'beginOffset': 10}, 'type': 'PROPER'}]},
            {'name': '#Election2020', 'type': 'OTHER', 'salience': 0.04056445136666298, 'mentions': [{'text': {'content': '#Election2020', 'beginOffset': 35}, 'type': 'PROPER'}]}], 'language': 'en'}
        
        logging.error(
            '_load_all_results is not implemented! returning dummy if cluster_id == 1234')
        if cluster_id==1234:
            return dummy_response
        return None

    def _generate_entity_list(self, ner_analysis_result):
        return [entity['name'] for entity in ner_analysis_result['entities']]

    def _analyze_entities(self, text_content):
        """
        Analyze Entities in a string using the GCP Language API

        Structure of the returned values can be seen here:
        https://cloud.google.com/natural-language/docs/basics

        Args:
        text_content The text content to analyze
        """
        logging.debug('making API call')

        # Available types: PLAIN_TEXT, HTML
        type_ = enums.Document.Type.PLAIN_TEXT

        # Optional. If not specified, the language is automatically detected.
        # For list of supported languages:
        # https://cloud.google.com/natural-language/docs/languages
        language = "en"
        document = {"content": text_content,
                    "type": type_, "language": language}

        # Available values: NONE, UTF8, UTF16, UTF32
        encoding_type = enums.EncodingType.UTF8

        response = self.client.analyze_entities(
            document, encoding_type=encoding_type)
        return MessageToDict(response)

    def get_cluster_text(self, cluster_id):
        """ Get a Canonical Text result for a given cluster_id """
        # TODO: Implement this
        placeholder_text = 'Kermit vs John McCain for President. #StrangerThanFiction'
        logging.error(
            "get_cluster_text is not implemented! Returning placeholder text: '%s'", placeholder_text)
        return placeholder_text

    def get_entity_list_for_clusters(self, cluster_ids):
        """ For ad clusters, get an entity: [cluster_id] map for further analysis"""
        entity_cluster_map = defaultdict(list)
        for cluster_id in cluster_ids:
            cluster_text = self.get_cluster_text(cluster_id)

            # Always try to fetch a result from storage if possible.
            ner_analysis_result = self._load_all_results( cluster_id)
            if not ner_analysis_result:
                ner_analysis_result = self._analyze_entities(cluster_text)

            self._store_all_results(cluster_id, ner_analysis_result)

            entity_list = self._generate_entity_list(ner_analysis_result)

            for entity in entity_list:
                entity_cluster_map[entity].append(cluster_id)

        return entity_cluster_map

def generate_entity_cluster_report():
    # TODO: This if False is gated by productionization
    if False:
        config = generic_fb_collector.get_config(sys.argv[1])
        country_code = config['SEARCH']['COUNTRY_CODE'].lower()
        connection = generic_fb_collector.get_db_connection(config)
        db_interface = db_functions.DBInterface(connection)
        cluster_ids = db_interface.cluster_ids(country_code, '01312020', '02292020')
    else:
        cluster_ids = [1234, 123]

    analysis = NamedEntityAnalysis()
    entity_map = analysis.get_entity_list_for_clusters(cluster_ids)
    logging.debug('Analysis result: %s', entity_map)

    # TODO: This should write to GCS somewhere daily?
    with open(ENTITY_MAP_FILE, 'w') as outfile:
        json.dump(entity_map, outfile)
    



if __name__ == '__main__':
    generate_entity_cluster_report()