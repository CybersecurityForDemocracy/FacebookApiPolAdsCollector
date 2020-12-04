"""Module to make batches of unfetched archive IDs and store them in the DB."""
import sys

import db_functions
import config_utils

DEFAULT_BATCH_SIZE = 1000
DEFAULT_MIN_AD_CREATION_DATE = '2019-01-01'

def main(config_path):
    config = config_utils.get_config(config_path)
    country_code = config.get('SEARCH', 'COUNTRY_CODE', fallback=None)
    min_ad_creation_date = config.get('SEARCH', 'MIN_AD_CREATION_DATE',
                                      fallback=DEFAULT_MIN_AD_CREATION_DATE)
    with config_utils.get_database_connection_from_config(config) as database_connection:
        database_interface = db_functions.DBInterface(database_connection)
        database_interface.make_snapshot_fetch_batches(batch_size=DEFAULT_BATCH_SIZE,
                                                       country_code=country_code,
                                                       min_ad_creation_date=min_ad_creation_date)

if __name__ == '__main__':
    config_utils.configure_logger('archive_id_batcher.log')
    main(sys.argv[1])
