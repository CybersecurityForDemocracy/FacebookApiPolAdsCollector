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


def main(config):
    database_connection_params = config_utils.get_database_connection_params_from_config(config)
    all_clusters = text_clustering_utils.update_ad_creative_clusters(database_connection_params)
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
