"""Module to perform text and image clustering based on hash simialirty.
"""
import configparser
import json
import logging
import sys
import random

import psycopg2
import simhash

import db_functions
import config_utils
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
    with get_db_connection(config) as db_connection:
        logging.info('DB connection %s', db_connection.dsn)

        all_clusters = text_clustering_utils.update_ad_creative_clusters(db_connection)
        all_simhash_clusters_as_lists = [list(cluster) for cluster in all_simhash_clusters]
        # TODO(macpd): persist clusters somehow.
        all_clusters_filename = 'all_clusters.json'
        with open(all_clusters_filename, 'w') as f:
            json.dump(all_simhash_clusters_as_lists, f)
        print(f'Wrote all simhash clusters as JSON to {all_clusters_filename}')


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read(sys.argv[1])

    config_utils.configure_logger("find_text_and_image_clusters.log")
    main(config)
