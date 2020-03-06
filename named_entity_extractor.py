""" Wrapper for all GCP Language API code.

You can read more about it here: https://cloud.google.com/natural-language/docs/basics
You can generate a new credentials file here: https://console.cloud.google.com/apis/credentials/serviceaccountkey?project=nyupoladstransparency&folder=&organizationId=&angularJsUrl=%2Fapis%2Fcredentials%2Fserviceaccountkey%3Fsupportedpurview%3Dproject%26project%3Dnyupoladstransparency%26folder%3D%26organizationId%3D&supportedpurview=project
"""
import logging
import sys
from collections import defaultdict, namedtuple

from google.cloud import language_v1
from google.cloud.language_v1 import enums
from google.protobuf.json_format import MessageToDict

import db_functions
import config_utils

GCS_CREDENTIALS_FILE = 'credentials.json'
ENTITY_MAP_FILE = 'map_for_date.json'
MAX_TEXT_LENGTH_FOR_NER_ANALYSIS = 1000

AdCreativeToRecognizedEntityRecord = namedtuple('AdCreativeToRecognizedEntityRecord',
                                                ['ad_creative_id', 'entity_id'])


class NamedEntityAnalysis:
    """ This Class Handles All aspects of the Name Entity Recognition task.

    No GCP Language API specific datastructures/code should escape this class.
    """

    def __init__(self, database_connection, credentials_file=GCS_CREDENTIALS_FILE):
        self.database_connection = database_connection
        self.database_interface = db_functions.DBInterface(database_connection)
        self.language_service_client = language_v1.LanguageServiceClient.from_service_account_json(
            credentials_file)

    def _store_all_results(self, text_sha256_hash, ner_analysis_result):
        """ Store complete result for deeper analysis as needed later. """
        self.database_interface.insert_named_entity_recognition_results(text_sha256_hash,
                                                                        ner_analysis_result)

    def _load_all_results(self, text_sha256_hash):
        """Load complete results for specified ad creative body text sha256 hash."""
        return self.database_interface.get_stored_recognized_entities_for_text_sha256_hash(
            text_sha256_hash)

    def _generate_entity_set(self, ner_analysis_result):
        if 'entities' not in ner_analysis_result:
            return set()

        return {db_functions.EntityRecord(name=entity['name'], type=entity['type']) for entity in
                ner_analysis_result['entities']}


    def _analyze_entities(self, text_content):
        """
        Analyze Entities in a string using the GCP Language API

        Structure of the returned values can be seen here:
        https://cloud.google.com/natural-language/docs/basics

        Args:
            text_content: str The text content to analyze
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

    def analyze_entities_in_texts(self, unique_ad_body_texts):
        """
        For ad creative body texts, analyze text for named entites, and store results for further
        analysis.

        NER analysis for a given text is first looked for in storage. If not present the text is
        sent for analysis via google language service API. Then the analysis is stored for later
        use.

        Entites and the relationship of which entities appear in which ad creative body texts is
        also stored.

        Args:
            unique_ad_body_texts: dict of sha256 hash -> ad creative body text to analyze for named
            entities.
        """
        entity_to_ad_creative_ids = defaultdict(list)
        for text_sha256_hash in unique_ad_body_texts:

            # Always try to fetch a result from storage if possible.
            ner_analysis_result = self._load_all_results(text_sha256_hash)
            logging.debug('Got NER analysis for ad creative body text sha256 hash %s in DB: %s',
                          text_sha256_hash, ner_analysis_result)
            if not ner_analysis_result:
                text = unique_ad_body_texts[text_sha256_hash]
                if len(text) > MAX_TEXT_LENGTH_FOR_NER_ANALYSIS:
                    text = text[:MAX_TEXT_LENGTH_FOR_NER_ANALYSIS]
                ner_analysis_result = self._analyze_entities(unique_ad_body_texts[text_sha256_hash])
                logging.info(
                    'Got NER analysis for text sha256 hash %s from google:\n%s', text_sha256_hash,
                    ner_analysis_result)

            self._store_all_results(text_sha256_hash, ner_analysis_result)
            self.database_connection.commit()

            entity_set = self._generate_entity_set(ner_analysis_result)
            ad_creative_ids = self.database_interface.ad_creative_ids_with_text_sha256_hash(
                text_sha256_hash)

            for entity in entity_set:
                entity_to_ad_creative_ids[entity].extend(ad_creative_ids)

        self._update_recognized_entities_in_database(entity_to_ad_creative_ids)

        return entity_to_ad_creative_ids

    def _update_recognized_entities_in_database(self, entity_to_ad_creative_ids):
        """Store new entites in DB, and update/insert entity to ad creative relationships in DB.

        Args:
            entity_to_ad_creative_ids: dict db_functions.EntityRecord -> list of ad_creative_id in
            which those entites were found.
        """
        existing_entities = self.database_interface.existing_recognized_entities()
        new_entities = set(entity_to_ad_creative_ids.keys()) - set(existing_entities.keys())
        if new_entities:
            logging.info('Got %d new entities.', len(new_entities))
            self.database_interface.insert_recognized_entities(new_entities)
            existing_entities = self.database_interface.existing_recognized_entities()

        ad_creative_to_recognized_entities_records = []
        for entity, ad_creative_ids in entity_to_ad_creative_ids.items():
            for ad_creative_id in ad_creative_ids:
                ad_creative_to_recognized_entities_records.append(
                    AdCreativeToRecognizedEntityRecord(ad_creative_id=ad_creative_id,
                                                       entity_id=existing_entities[entity]))
        self.database_interface.insert_ad_recognized_entity_records(
            ad_creative_to_recognized_entities_records)
        self.database_connection.commit()

def generate_entity_report():
    config = config_utils.get_config(sys.argv[1])
    country_code = config['SEARCH']['COUNTRY_CODE'].lower()
    with config_utils.get_database_connection_from_config(config) as database_connection:
        db_interface = db_functions.DBInterface(database_connection)
        # TODO(macpd): pull these dates from somewhere. config, database, interval to-from current
        # date, etc
        unique_ad_body_texts = db_interface.unique_ad_body_texts(
            country_code, '2020-01-01', '2020-03-31')

        logging.info('Got %d unique ad body_texts.', len(unique_ad_body_texts))

        analysis = NamedEntityAnalysis(database_connection=database_connection,
                                       credentials_file='gcs_credentials.json')
        analysis.analyze_entities_in_texts(unique_ad_body_texts)


if __name__ == '__main__':
    config_utils.configure_logger('named_entity_extractor.log')
    generate_entity_report()
