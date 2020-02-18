import logging


def ConfigureLogger(log_filename):
    record_format = (
        '[%(levelname)s\t%(asctime)s] %(process)d %(thread)d {%(filename)s:%(lineno)d} '
        '%(message)s')
    logging.basicConfig(
        handlers=[logging.FileHandler(log_filename),
                  logging.StreamHandler()],
        format=record_format,
        level=logging.INFO)
