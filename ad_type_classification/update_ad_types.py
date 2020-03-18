"""Module to classify ad types via precomputed models and update the types in the database.
"""
from collections import defaultdict
import json
import logging
import os.path
import sys

import pandas
import joblib

import config_utils
import db_functions
from ad_type_classification.helper_fns import (get_creative_url, get_lookup_table)
from ad_type_classification.text_process_fns import process_creative_body

DATA_DIR = os.path.join('ad_type_classification', 'data')

def main(config_file_path):
    config = config_utils.get_config(config_file_path)
    with config_utils.get_database_connection_from_config(config) as database_connection:

        classifier = joblib.load(os.path.join(DATA_DIR, 'ad_type_classifier.pk1'))
        label_encoder = joblib.load(os.path.join(DATA_DIR, 'ad_type_label_encoder.pk1'))
        data_prep_pipeline = joblib.load(os.path.join(DATA_DIR, 'data_prep_pipeline.pk1'))
        db_interface = db_functions.DBInterface(database_connection)

        ad_type_to_archive_ids = defaultdict(list)
        lookup_table = get_lookup_table(os.path.join(DATA_DIR, 'ad_url_to_type.csv'))
        ad_iterator = classify_ad_iterator(
            db_interface.all_ads_with_nonempty_link_caption_or_body(), lookup_table, classifier,
            label_encoder, data_prep_pipeline)
        for classified_ad in ad_iterator:
            ad_type_to_archive_ids[classified_ad['ad_type']].append(classified_ad['archive_id'])
        type_sums = ['%s: %d' % (k, len(v)) for k, v in ad_type_to_archive_ids.items()]
        logging.info('Type sums:\n%s', '\n'.join(type_sums))
        with open('ad_to_type_mappings.json', 'w') as ad_mapping_file:
            json.dump(ad_type_to_archive_ids, ad_mapping_file)

        db_interface.update_ad_types(archive_id_and_ad_type_iterator(ad_type_to_archive_ids))
        database_connection.commit()


def classify_ad_iterator(ads, lookup_table, classifier, label_encoder, data_prep_pipeline):
    """Classifies ads in batches, and yields results.

    Args:
        ads: dict of ads with archive_id, ad_creative_body, ad_creative_link_caption
        lookup_table: dict normalized_url -> ad_type.
        classifier: model for classifying ad type from ad body.
        label_encoder: model for translating internal representation of ad type to ad type string.
        data_prep_pipeline: model for preparing ad body for classification.
    Yields:
        dict of classified add with keys 'archive_id' and 'ad_type'.
    """
    to_classify = []
    num_rows_processed = 0
    for result in ads:
        normalized_url = get_creative_url(result)
        if normalized_url in lookup_table:
            yield {'archive_id': result['archive_id'],
                   'ad_type': lookup_table.get(normalized_url)}
        elif result['ad_creative_body']:
            to_classify.append(result)
        else:
            yield {'archive_id': result['archive_id'],
                   'ad_type': 'UNKNOWN'}
        if len(to_classify) >= 100000:
            logging.info('Classifying batch of %d. classify_ad_iterator processed %d rows.',
                         len(to_classify), num_rows_processed)
            classification_df = pandas.DataFrame(to_classify)
            classification_df['processed_body'] = classification_df['ad_creative_body'].apply(
                process_creative_body)
            prediction_data = data_prep_pipeline.transform(
                classification_df['processed_body'].astype('str'))
            classification_df['ad_type'] = classifier.predict(prediction_data)
            classification_df['ad_type'] = label_encoder.inverse_transform(
                classification_df['ad_type'])
            for classified_ad in classification_df.to_dict(orient='records'):
                yield {'archive_id': classified_ad['archive_id'],
                       'ad_type': classified_ad['ad_type'].upper()}
            to_classify = []

        num_rows_processed += 1


def archive_id_and_ad_type_iterator(ad_type_to_archive_ids):
    """Flattens ad type to archive id list map to tuples of (id, type).

    Yields:
        tuple of (archive_id, ad_type).
    """
    for ad_type in ad_type_to_archive_ids:
        for archive_id in ad_type_to_archive_ids[ad_type]:
            yield (archive_id, ad_type)


if __name__ == '__main__':
    config_utils.configure_logger("update_ad_types.log")
    if len(sys.argv) < 2:
        print(f"Usage: python3 {sys.argv[0]} update_ad_types.cfg")
        sys.exit(1)
    main(sys.argv[1])
