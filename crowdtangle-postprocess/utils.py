import os
import configparser
import argparse


def load_config(config_path=None):
    # loading .cfg config file
    if config_path is None:
        config_path = './crowdtangle-postprocess/ukraine_crowdtangle_config.cfg'
    # configDict = {}
    config = configparser.ConfigParser()
    config.read(config_path)
    # print(config.get('DATABASE','URI'))
    # os.environ.update(config)
    return config