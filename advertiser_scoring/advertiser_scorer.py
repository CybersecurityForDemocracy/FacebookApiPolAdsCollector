import sys

import config_utils
import db_functions

def main(config_path):
    config = config_utils.get_config(config_path)
    db_connection_params = config_utils.get_database_connection_params_from_config(config)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit('Usage: %s <config file>' % sys.argv[0])
    config_utils.configure_logger("advertiser_scorer.log")
    main(sys.argv[1])
