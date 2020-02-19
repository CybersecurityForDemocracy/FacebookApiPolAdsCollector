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

    db_interface = db_functions.DBInterface(db_connection)
    all_text_simhashes = db_interface.all_ad_creative_text_simhashes()
    archive_id_to_simhash = {}
    for a in all_text_simhashes:
        archive_id_to_simhash[str(a)] = simhash.Simhash(int(all_text_simhashes[a],16))
    text_simhash_index = simhash.SimhashIndex(archive_id_to_simhash.items(), k=3)
    seen_archive_ids = set()
    archive_ids_with_similar_text = []
    for archive_id in archive_id_to_simhash:
        if len(seen_archive_ids) % 10000 == 0:
            logging.info('Processed %d archive IDs.', len(seen_archive_ids))
        if archive_id in seen_archive_ids:
            continue

        seen_archive_ids.add(archive_id)
        archive_ids_with_similar_text.append(text_simhash_index.get_near_dups(archive_id_to_simhash[archive_id]))


    logging.info('Processed %d archive IDs. got %d clusters', len(seen_archive_ids),
                 len(archive_ids_with_similar_text))
    logging.info('First 3 sets of archive IDs with similar text: %s',
                 archive_ids_with_similar_text[:3])
    random_archive_id_set = random.choice(archive_ids_with_similar_text)
    logging.info('Archive IDs %s have similar text simhashes', random_archive_id_set)




if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read(sys.argv[1])
    logging.basicConfig(handlers=[logging.FileHandler("text_clustering_utils.log"),
                              logging.StreamHandler()],
                        format='[%(levelname)s\t%(asctime)s] {%(pathname)s:%(lineno)d} %(message)s',
                        level=logging.INFO)
    main(config)
