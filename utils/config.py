import json
import logging.config
from pathlib import Path

CONFIG_DIR_PATTERN = str(Path(__file__).parents[1]) + '/config/config.{config_file}.json'
CONFIG_NAME_DATA = 'data'
CONFIG_NAME_LOCAL = 'local'


def get_config(config_name):
    with open(CONFIG_DIR_PATTERN.format(config_file=config_name), "r") as config_file:
        return json.load(config_file)

def get_local_data_folder(data_name):
    # validate the data name has a config for it
    config_data = get_config(CONFIG_NAME_DATA)
    if not (data_name in config_data):
        raise Exception('data_name ' + data_name + ' must be detailed in the config.data.json file')
    config_local = get_config(CONFIG_NAME_LOCAL)
    return config_local['data_folder'] + data_name + '/'
