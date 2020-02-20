"""Sample module to get snapshot URLs for a random simhash that appears 2 or more times in database.

prints list of URLs (one per line).
"""
import configparser
import logging
import sys
import random

import psycopg2
import simhash

import db_functions
import standard_logger_config
from ad_reporting_tool import text_clustering_utils

def get_db_connection(config):
    host = config['POSTGRES']['HOST']
    dbname = config['POSTGRES']['DBNAME']
    user = config['POSTGRES']['USER']
    password = config['POSTGRES']['PASSWORD']
    port = config['POSTGRES']['PORT']
    dbauthorize = "host=%s dbname=%s user=%s password=%s port=%s" % (
        host, dbname, user, password, port)
    return psycopg2.connect(dbauthorize)

def main(config):
    db_connection = get_db_connection(config)
    logging.info('DB connection %s', db_connection.dsn)

    archive_ids_with_similar_text = text_clustering_utils.ad_creative_body_text_similarity_clusters(db_connection)
    logging.info('First 3 sets of archive IDs with similar text: %s',
                 archive_ids_with_similar_text[:3])
    random_archive_id_set = random.choice(archive_ids_with_similar_text)
    while len(random_archive_id_set) == 1:
        random_archive_id_set = random.choice(archive_ids_with_similar_text)
    logging.info('Archive IDs %s have similar text simhashes', random_archive_id_set)

    archive_ids_with_similar_image = text_clustering_utils.ad_creative_image_similarity_clusters(db_connection)
    archive_ids_with_similar_image_without_optimization = text_clustering_utils.ad_creative_image_similarity_clusters_without_optimization(db_connection)
    logging.info('archive_ids_with_similar_image == archive_ids_with_similar_image_without_optimization %s',
                 (archive_ids_with_similar_image ==
                 archive_ids_with_similar_image_without_optimization))
    logging.info('First 3 sets of archive IDs with similar image: %s',
                 archive_ids_with_similar_image[:3])
    random_archive_id_set = random.choice(archive_ids_with_similar_image)
    while len(random_archive_id_set) == 1:
        random_archive_id_set = random.choice(archive_ids_with_similar_image)
    logging.info('Archive IDs %s have similar image simhashes', random_archive_id_set)



if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read(sys.argv[1])

    standard_logger_config.configure_logger("text_clustering_utils.log")
    main(config)
