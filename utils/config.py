import json
import logging.config
from pathlib import Path

CONFIG_DIR_PATTERN = str(Path(__file__).parents[1]) + '/config/config.{config_file}.json'
CONFIG_NAME_DATA = 'data'
CONFIG_NAME_PATHS = 'paths'

class DataSetNames():
    hdgp = 'hgdp'

def get_config(config_name):
    with open(CONFIG_DIR_PATTERN.format(config_file=config_name), "r") as config_file:
        return json.load(config_file)

def get_num_individuals(data_set_name=DataSetNames.hdgp):
    data_config = get_config(CONFIG_NAME_DATA)
    return data_config[data_set_name]['num_individulas']

def get_num_chrs(data_set_name=DataSetNames.hdgp):
    data_config = get_config(CONFIG_NAME_DATA)
    return data_config[data_set_name]['num_chrs']

def get_sample_sites_file_name(data_set_name=DataSetNames.hdgp):
    data_config = get_config(CONFIG_NAME_DATA)
    return data_config[data_set_name]['sample_sites_file_name']

def get_indlist_file_name(data_set_name=DataSetNames.hdgp):
    data_config = get_config(CONFIG_NAME_DATA)
    return data_config[data_set_name]['indlist_file_name']

def get_cluster_data_folder():
    data_config = get_config(CONFIG_NAME_PATHS)
    return data_config['cluster_data_folder']

