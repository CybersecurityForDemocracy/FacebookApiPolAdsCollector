"""Module to classify ad types via precomputed models and update the types in the database.
"""
from collections import defaultdict
import json
import logging
import os.path
import re
import sys
from urllib import parse

import joblib
import pandas

import config_utils
import db_functions
from ad_type_classification import ad_creative_body_processor

DATA_DIR = os.path.join('ad_type_classification', 'data')
URL_RE = re.compile(
        r'http[s]?://(?:[a-zA-Z]|[0-9]|[$\-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')


def _get_canonical_url(url):
    """Parse string for URL, and strip www. if present.

    Args:
        url: str of URL to parse.
    Returns:
        str of URL (with www. stripped if present) of parsed URL. empty string if parsing failed.
    """
    try:
        parsed_url = parse.urlparse(str(url))
        domain = str(parsed_url.netloc).lower()
        path = str(parsed_url.path).lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain + path
    except ValueError as err:
        logging.info('%s error while processing url: %s', err, url)
        return ''


def get_normalized_creative_url(row):
    """Get URL from ad creative data. Either link caption or parsed from creative body.

    Args:
        row: dict with keys 'ad_creative_link_caption' and 'ad_creative_body'.
    Returns:
        string: domain (ie secure.actblue.com) of link in ad creative.
    """
    url = ''
    if row.get('ad_creative_link_caption'):
        url = row['ad_creative_link_caption']
    else:
        match = re.search(URL_RE, row['ad_creative_body'])
        if match:
            url = _get_canonical_url(match.group(0))
    return url


def get_canonical_url_to_ad_type_table(canonical_url_to_type_csv_path):
    """Returns dict of canonical normalized_url -> ad_type.

    Args:
        str path to CSV of ad url and ad type data.
    Returns:
        dict normalized_url -> ad_type.
    """
    canonical_url_to_type_df = pandas.read_csv(canonical_url_to_type_csv_path)
    canonical_url_to_type_df['ad_type'] = canonical_url_to_type_df['ad_type'].astype('category')
    df_as_dicts = canonical_url_to_type_df.to_dict(orient='records')
    canonical_url_to_ad_type_table = {}
    for row in df_as_dicts:
        canonical_url_to_ad_type_table[row['normalized_url']] = row['ad_type']
    canonical_url_to_ad_type_table[''] = 'INFORM'
    return canonical_url_to_ad_type_table


def classify_ad_iterator(ads, canonical_url_to_ad_type_table, classifier, label_encoder,
                         data_prep_pipeline):
    """Classifies ads in batches, and yields results.

    Args:
        ads: dict of ads with archive_id, ad_creative_body, ad_creative_link_caption
        canonical_url_to_ad_type_table: dict normalized_url -> ad_type.
        classifier: model for classifying ad type from ad body.
        label_encoder: model for translating internal representation of ad type to ad type string.
        data_prep_pipeline: model for preparing ad body for classification.
    Yields:
        dict of classified add with keys 'archive_id' and 'ad_type'.
    """
    processor = ad_creative_body_processor.AdCreativeBodyProcessor()
    to_classify = []
    num_rows_processed = 0
    for result in ads:
        normalized_url = get_normalized_creative_url(result)
        if normalized_url in canonical_url_to_ad_type_table:
            yield {'archive_id': result['archive_id'],
                   'ad_type': canonical_url_to_ad_type_table.get(normalized_url)}
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
                processor.process_creative_body)
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


def main(config_file_path):
    """Perform ad classification.

    Args:
        config_file_path: str path to config file.
    """
    config = config_utils.get_config(config_file_path)
    classifier = joblib.load(os.path.join(DATA_DIR, 'ad_type_classifier.pk1'))
    label_encoder = joblib.load(os.path.join(DATA_DIR, 'ad_type_label_encoder.pk1'))
    data_prep_pipeline = joblib.load(os.path.join(DATA_DIR, 'data_prep_pipeline.pk1'))

    with config_utils.get_database_connection_from_config(config) as database_connection:

        db_interface = db_functions.DBInterface(database_connection)

        ad_type_to_archive_ids = defaultdict(list)
        canonical_url_to_ad_type_table = get_canonical_url_to_ad_type_table(os.path.join(
            DATA_DIR, 'ad_url_to_type.csv'))
        ad_iterator = classify_ad_iterator(
            db_interface.all_ads_with_nonempty_link_caption_or_body(),
            canonical_url_to_ad_type_table, classifier,
            label_encoder, data_prep_pipeline)
        for classified_ad in ad_iterator:
            ad_type_to_archive_ids[classified_ad['ad_type']].append(classified_ad['archive_id'])
        type_sums = ['%s: %d' % (k, len(v)) for k, v in ad_type_to_archive_ids.items()]
        logging.info('Type sums:\n%s', '\n'.join(type_sums))
        with open('ad_to_type_mappings.json', 'w') as ad_mapping_file:
            json.dump(ad_type_to_archive_ids, ad_mapping_file)

        db_interface.update_ad_types(archive_id_and_ad_type_iterator(ad_type_to_archive_ids))
        database_connection.commit()


if __name__ == '__main__':
    config_utils.configure_logger("update_ad_types.log")
    if len(sys.argv) < 2:
        print(f"Usage: python3 {sys.argv[0]} update_ad_types.cfg")
        sys.exit(1)
    main(sys.argv[1])
