import sys

import db_functions
import config_utils

def main(config):
    database_connection_params = config_utils.get_database_connection_params_from_config(config)
    with config_utils.get_database_connection(database_connection_params) as database_connection:
        database_interface = db_functions.DBInterface(database_connection)
        database_interface.make_snapshot_fetch_batches()

if __name__ == '__main__':
    config = config_utils.get_config(sys.argv[1])
    main(config)
