"""Module to hold common config logic."""
import collections
import configparser
import logging

import psycopg2

DatabaseConnectionParams = collections.namedtuple('DatabaseConnectionParams',
                                                  ['host',
                                                   'database_name',
                                                   'username',
                                                   'password',
                                                   'port'])


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
        port=config['POSTGRES']['PORT'])


def get_database_connection(database_connection_params):
    """Get pyscopg2 database connection using the provided params.

    Args:
        database_connection_params: DatabaseConnectionParams object from which to pull connection
        params.
    Returns:
        psycopg2.connection ready to be used.
    """
    db_authorize = ("host=%(host)s dbname=%(database_name)s user=%(username)s "
                    "password=%(password)s port=%(port)s") % database_connection_params._asdict()
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
