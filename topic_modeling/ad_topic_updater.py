"""Module to assign topic(s) to ads based on keyword(s) that occur in ad creative body text."""
from collections import namedtuple
import datetime
import logging
import sys

import pandas as pd

import config_utils
import db_functions


AdTopicRecord = namedtuple('AdTopicRecord', ['topic_id', 'archive_id'])

def main(argv):
    config = config_utils.get_config(argv[0])

    database_connection_params = config_utils.get_database_connection_params_from_config(config)
    with config_utils.get_database_connection(database_connection_params) as db_connection:
        db_interface = db_functions.DBInterface(db_connection)
        archive_id_and_ad_body = db_interface.ad_body_texts(
            'US', start_time=None, end_time=None)
        logging.info('Got %d ad_creative_bodies to analyze.', len(archive_id_and_ad_body))
        keyword_df = pd.read_csv('topic_modeling/keyword_topic_map.csv')
        logging.info('Got %d topics, and %d keywords.', len(set(keyword_df['topic'])),
                     len(keyword_df))
        # Insert topics from CSV in case they aren't in the DB yet.
        db_interface.insert_topic_names(set(keyword_df['topic']))

        archive_ids = []
        texts = []
        [(archive_ids.append(i), texts.append(j)) for i, j in archive_id_and_ad_body]

        ads_df = pd.DataFrame(data={'archive_id':
                                        pd.Series(archive_ids),
                                    'ad_creative_body':
                                        pd.Series(texts)}).dropna(axis=1, how='all')

        text_to_archive_id = ads_df.groupby(['ad_creative_body'])['archive_id'].apply(
            lambda group_series: group_series.tolist()).reset_index()

        topic_to_keyword = keyword_df.groupby(['topic'])['keyword'].apply(
            lambda group_series: group_series.tolist()).reset_index()

        # Map topic -> list of archive_id where ad_creative_body contains keyword for topic.
        topic_to_archive_ids = topic_to_keyword.apply(
            lambda x:
            pd.Series([x.topic,
                       text_to_archive_id[text_to_archive_id.ad_creative_body.str.contains(
                           '|'.join(x.keyword), na=False,
                           regex=True)].archive_id.sum()],
                      index=['topic', 'archive_ids']), axis=1)

        # Get map of topic name -> topic ID
        topic_name_to_id = db_interface.all_topics()
        ad_topic_records = []
        for topic_name, archive_ids in zip(topic_to_archive_ids['topic'],
                                           topic_to_archive_ids['archive_ids']):
            topic_id = topic_name_to_id[topic_name]
            if archive_ids:
                ad_topic_records.extend(
                    [AdTopicRecord(topic_id=topic_id, archive_id=archive_id)
                     for archive_id in archive_ids])
        logging.info('Inserting %d topic ID -> archive ID relationships.', len(ad_topic_records))
        db_interface.insert_ad_topics(ad_topic_records)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit('Usage: %s <config file>' % sys.argv[0])
    config_utils.configure_logger("ad_topic_updater.log")
    main(sys.argv[1:])
