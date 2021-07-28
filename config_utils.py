"""Module to hold common config logic."""
import collections
import configparser
import logging

import psycopg2
import tenacity

LOGGER = logging.getLogger(__name__)
DatabaseConnectionParams = collections.namedtuple('DatabaseConnectionParams',
                                                  ['host',
                                                   'database_name',
                                                   'username',
                                                   'password',
                                                   'port',
                                                   'default_schema',
                                                   'sslrootcert',
                                                   'sslcert',
                                                   'sslkey',
                                                   ])


def get_database_connection_params_from_config(config):
    """Get database connection params from configparser object.

    Args:
        config: configparser.ConfigParser from which to extract config params.
    Returns:
        DatabaseConnectionParams to get a database connection.
    """
    return DatabaseConnectionParams(
        host=config['POSTGRES']['HOST'],
        database_name=config['POSTGRES']['DBNAME'],
        username=config['POSTGRES']['USER'],
        password=config['POSTGRES']['PASSWORD'],
        port=config['POSTGRES']['PORT'],
        default_schema=config.get('POSTGRES', 'SCHEMA', fallback=None),
        sslrootcert=config.get('POSTGRES', 'SERVER_CA', fallback=None),
        sslcert=config.get('POSTGRES', 'CLIENT_CERT', fallback=None),
        sslkey=config.get('POSTGRES', 'CLIENT_KEY', fallback=None))



@tenacity.retry(stop=tenacity.stop_after_attempt(4),
                wait=tenacity.wait_random_exponential(multiplier=1, max=120),
                retry=tenacity.retry_if_exception_type(psycopg2.OperationalError),
                reraise=True,
                before_sleep=tenacity.before_sleep_log(LOGGER, logging.INFO))
def _get_database_connection_with_retry(db_authorize):
    return psycopg2.connect(db_authorize)

def get_database_connection(database_connection_params, retry=True):
    """Get pyscopg2 database connection using the provided params.

    Args:
        database_connection_params: DatabaseConnectionParams object from which to pull connection
            params.
        retry: If connection fails due to operational error retry upto 2 additional times.
    Returns:
        psycopg2.connection ready to be used.
    """
    db_authorize = (
        "host=%(host)s dbname=%(database_name)s user=%(username)s password=%(password)s "
        "port=%(port)s sslmode=require") % database_connection_params._asdict()
    if database_connection_params.default_schema:
        db_authorize += (
            ' options=-csearch_path=%(default_schema)s' % database_connection_params._asdict())
    if any([database_connection_params.sslrootcert, database_connection_params.sslcert,
           database_connection_params.sslkey]):
        db_authorize += (
            " sslmode=verify-ca sslrootcert=%(sslrootcert)s sslcert=%(sslcert)s sslkey=%(sslkey)s"
            ) % database_connection_params._asdict()
    if retry:
        connection = _get_database_connection_with_retry(db_authorize)
    else:
        connection = psycopg2.connect(db_authorize)
    logging.info('Established connecton to %s', connection.dsn)
    return connection


def get_database_connection_from_config(config):
    """Get pyscopg2 database connection from the provided ConfigParser.

    Args:
        config: configparser.ConfigParser initialized from desired file.
    Returns:
        psycopg2.connection ready to be used.
    """
    connection_params = get_database_connection_params_from_config(config)
    return get_database_connection(connection_params)


def get_facebook_access_token(config):
    return config['FACEBOOK']['TOKEN']


def get_config(config_path):
    """Get configparser object initialized from config path.

    Args:
        config_path: str file path to config.
    Returns:
        configparser.ConfigParser initialized from config_path.
    """
    config = configparser.ConfigParser()
    config.read(config_path)
    return config


def configure_logger(log_filename):
    """Configure root logger to write to log_filename and STDOUT.

    Args:
      log_filename: str, filename to be used for log file.
    """
    record_format = (
        '[%(levelname)s\t%(asctime)s] %(process)d %(thread)d {%(filename)s:%(lineno)d} '
        '%(message)s')
    logging.basicConfig(
        handlers=[logging.FileHandler(log_filename),
                  logging.StreamHandler()],
        format=record_format,
        level=logging.INFO)
