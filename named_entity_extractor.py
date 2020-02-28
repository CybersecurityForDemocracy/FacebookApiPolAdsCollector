""" Wrapper for all GCP Language API code.

You can read more about it here: https://cloud.google.com/natural-language/docs/basics
You can generate a new credentials file here: https://console.cloud.google.com/apis/credentials/serviceaccountkey?project=nyupoladstransparency&folder=&organizationId=&angularJsUrl=%2Fapis%2Fcredentials%2Fserviceaccountkey%3Fsupportedpurview%3Dproject%26project%3Dnyupoladstransparency%26folder%3D%26organizationId%3D&supportedpurview=project
"""
import logging
import sys
import json
from collections import defaultdict, namedtuple

from google.cloud import language_v1
from google.cloud.language_v1 import enums
from google.protobuf.json_format import MessageToDict

import db_functions
import config_utils

GCS_CREDENTIALS_FILE = 'credentials.json'
ENTITY_MAP_FILE = 'map_for_date.json'


class NamedEntityAnalysis(object):
    """ This Class Handles All aspects of the Name Entity Recognition task.

    No GCP Language API specific datastructures/code should escape this class.
    """

    def __init__(self, database_connection, credentials_file=GCS_CREDENTIALS_FILE):
        self.database_connection = database_connection
        self.database_interface = db_functions.DBInterface(database_connection)
        self.language_service_client = language_v1.LanguageServiceClient.from_service_account_json(credentials_file)

    def _store_all_results(self, text_sha256_hash, ner_analysis_result):
        """ Store complete result for deeper analysis as needed later. """
        self.database_interface.insert_named_entity_recognition_results(text_sha256_hash,
                                                                        ner_analysis_result)

    def _load_all_results(self, text_sha256_hash):
        """Load complete results for specified ad creative body text sha256 hash."""
        return self.database_interface.get_stored_recognized_entities_for_text_sha256_hash(
                text_sha256_hash)

    def _generate_entity_set(self, ner_analysis_result):
        return {entity['name'] for entity in ner_analysis_result['entities']}

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

        response = self.language_service_client.analyze_entities(
            document, encoding_type=encoding_type)
        return MessageToDict(response)

    def get_entity_list_for_texts(self, unique_ad_body_texts):
        """ For ad creative body texts, get an entity -> [text_sha256_hash] map for further analysis"""
        entity_to_text_hash_map = defaultdict(list)
        for text_sha256_hash in unique_ad_body_texts:

            # Always try to fetch a result from storage if possible.
            ner_analysis_result = self._load_all_results(text_sha256_hash)
            logging.debug('Got NER analysis for ad creative body text sha256 hash %s in DB: %s',
                          text_sha256_hash, ner_analysis_result)
            if not ner_analysis_result:
                ner_analysis_result = self._analyze_entities(unique_ad_body_texts[text_sha256_hash])
                logging.debug(
                        'Got NER analysis for text sha256 hash %s from google:\n%s',
                        text_sha256_hash, ner_analysis_result)

            self._store_all_results(text_sha256_hash, ner_analysis_result)

            entity_set = self._generate_entity_set(ner_analysis_result)

            for entity in entity_set:
                entity_to_text_hash_map[entity].append(text_sha256_hash)

        return entity_to_text_hash_map

def generate_entity_report():
    # TODO: This if False is gated by productionization
    config = config_utils.get_config(sys.argv[1])
    country_code = config['SEARCH']['COUNTRY_CODE'].lower()
    with config_utils.get_database_connection_from_config(config) as database_connection:
        if False:
            db_interface = db_functions.DBInterface(database_connection)
            unique_ad_body_texts = db_interface.unique_ad_body_texts(country_code, '2020-01-31',
                                                                     '2020-02-29')
        else:
            unique_ad_body_texts = (
            {'00001a8d8e6951f0efb9834bc7ed414d0f4479c4b31f210283164807a7a11c3a':
             'Don’t you deserve a voice? Urge lawmakers to fix how Iowa selects judges!',
             '00003b827123286da75d40d5b969ce9d03bfad6ce49244ad7a02c134ac30b6b7':
             ('Please contribute $45 in the NEXT 24 HOURS to stand with me against these Fake '
              'Impeachment Trials and your gift will automatically be DOUBLE-MATCHED.')})

        logging.info('Got %d unique ad body_texts: %s', len(unique_ad_body_texts),
                     unique_ad_body_texts)

        analysis = NamedEntityAnalysis(database_connection=database_connection,
                                       credentials_file='gcs_credentials.json')
        entity_map = analysis.get_entity_list_for_texts(unique_ad_body_texts)
        logging.debug('Analysis result: %s', entity_map)

    print(json.dumps(entity_map))
    # TODO: This should write to GCS somewhere daily?
    with open(ENTITY_MAP_FILE, 'w') as outfile:
        json.dump(entity_map, outfile)
    logging.info('Wrote entity map to %s', ENTITY_MAP_FILE)


if __name__ == '__main__':
    config_utils.configure_logger('named_entity_extractor.log', debug=True)
    generate_entity_report()
