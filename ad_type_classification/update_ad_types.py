from collections import defaultdict
import json
import logging
import os.path
import sys

import pandas
import psycopg2
import psycopg2.extras
import joblib

import config_utils
import db_functions
from ad_type_classification.helper_fns import (find_urls, get_creative_url, get_lookup_table)
from ad_type_classification.text_process_fns import process_creative_body

MODELS_DIR = os.path.join('ad_type_classification', 'data')

def main(config_file_path):
    config = config_utils.get_config(config_file_path)
    with config_utils.get_database_connection_from_config(config) as database_connection:

        classifier = joblib.load(os.path.join(MODELS_DIR, 'ad_type_classifier.pk1'))
        label_encoder = joblib.load(os.path.join(MODELS_DIR, 'ad_type_label_encoder.pk1'))
        data_prep_pipeline = joblib.load(os.path.join(MODELS_DIR, 'data_prep_pipeline.pk1'))
        db_interface = db_functions.DBInterface(database_connection)

        ad_type_map = defaultdict(list)
        lookup_table = get_lookup_table(os.path.join(MODELS_DIR, 'ad_url_to_type.csv'))
        for r in classify_ads(
                db_interface.all_ads_with_nonempty_link_caption_or_body(),
                lookup_table, classifier, label_encoder, data_prep_pipeline):
            ad_type_map[r['ad_type']].append(r['archive_id'])
        for k, v in ad_type_map.items():
            print(k, len(v))
        with open('ad_to_type_mappings.json','w') as w:
            json.dump(ad_type_map, w)

        db_interface.update_ad_types(list_id_and_types())
        database_connection.commit()


def classify_ads(ads, lookup_table, classifier, label_encoder, data_prep_pipeline):
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
        num_rows_processed += 1
        if len(to_classify) > 100000:
            logging.info('classify_ads processed %d rows', num_rows_processed)
            classification_df = pandas.DataFrame(to_classify)
            classification_df['processed_body'] = classification_df['ad_creative_body'].apply(
                process_creative_body)
            prediction_data = data_prep_pipeline.transform(
                classification_df['processed_body'].astype('str'))
            classification_df['ad_type'] = classifier.predict(prediction_data)
            classification_df['ad_type'] = label_encoder.inverse_transform(
                classification_df['ad_type'])
            for result in classification_df.to_dict(orient='records'):
                yield result
            to_classify = []


def list_id_and_types(ad_type_to_archive_ids):
    for ad_type in ad_type_to_archive_ids:
        for archive_id in ad_type_to_archive_ids[ad_type]:
            yield (archive_id, ad_type)


if __name__ == '__main__':
    config_utils.configure_logger("update_ad_types.log")
    if len(sys.argv) < 2:
        exit(f"Usage:python3 {sys.argv[0]} update_ad_types.cfg")
    main(sys.argv[1])
