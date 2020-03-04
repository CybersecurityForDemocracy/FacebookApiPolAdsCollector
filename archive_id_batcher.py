"""Module to make batches of unfetched archive IDs and store them in the DB."""
import logging
import sys

import db_functions
import config_utils

DEFAULT_BATCH_SIZE = 1000

def main(config):
    with config_utils.get_database_connection_from_config(config) as database_connection:
        database_interface = db_functions.DBInterface(database_connection)
        logging.info('About to make batches (size %d) of unfetched archive IDs.',
                     DEFAULT_BATCH_SIZE)
        database_interface.make_snapshot_fetch_batches(batch_size=DEFAULT_BATCH_SIZE)

if __name__ == '__main__':
    config_utils.configure_logger('archive_id_batcher.log')
    config = config_utils.get_config(sys.argv[1])
    main(config)
