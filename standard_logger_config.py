import datetime
import logging

def ConfigureLogger(log_filename, include_date=True):
    logging.basicConfig(
            handlers=[logging.FileHandler(log_filename), logging.StreamHandler()],
            format=('[%(levelname)s\t%(asctime)s] %(process)d %(thread)d {%(filename)s:%(lineno)d} '
                    '%(message)s'),
            level=logging.INFO)
