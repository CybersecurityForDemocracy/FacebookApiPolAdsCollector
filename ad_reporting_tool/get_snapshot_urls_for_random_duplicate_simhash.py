"""Sample module to get snapshot URLs for a random simhash that appears 2 or more times in database.

prints list of URLs (one per line).
"""
import configparser
import logging
import sys
import random

import psycopg2

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

    simhash_to_url = text_clustering_utils.duplicated_ad_creative_body_simhash_snapshot_urls(
            config['FACEBOOK']['TOKEN'], db_connection)
    logging.info('Got URLs for %d duplicated sim_hashes', len(simhash_to_url.keys()))
    rand_simhash = random.choice(list(simhash_to_url.keys()))
    logging.info('random simhash %s has URLs:\n%s', rand_simhash,
                 '\n'.join(simhash_to_url[rand_simhash]))



if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read(sys.argv[1])
    logging.basicConfig(handlers=[logging.FileHandler("text_clustering_utils.log"),
                              logging.StreamHandler()],
                        format='[%(levelname)s\t%(asctime)s] {%(pathname)s:%(lineno)d} %(message)s',
                        level=logging.INFO)
    main(config)
