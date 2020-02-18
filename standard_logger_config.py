"""Module to put logging configuration in a single place to be reused throughout project."""
import logging


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
