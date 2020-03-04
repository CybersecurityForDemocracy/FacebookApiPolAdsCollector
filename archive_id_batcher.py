"""Module to make batches of unfetched archive IDs and store them in the DB."""
import sys

import db_functions
import config_utils

def main(config):
    with config_utils.get_database_connection_from_config(config) as database_connection:
        database_interface = db_functions.DBInterface(database_connection)
        database_interface.make_snapshot_fetch_batches()

if __name__ == '__main__':
    config = config_utils.get_config(sys.argv[1])
    main(config)
