import json
import logging.config
from pathlib import Path
from enum import Enum

CONFIG_DIR_PATTERN = str(Path(__file__).parents[1]) + '/config/config.{config_file}.json'
CONFIG_NAME_DATA = 'data'
CONFIG_NAME_PATHS = 'paths'

class DataSetNames(Enum):
    hdgp = 'hgdp'
    hdgp_test = 'hgdp_test'

def validate_dataset_name(dataset_name):
    return dataset_name in [d.value for d in DataSetNames]

def get_config(config_name):
    with open(CONFIG_DIR_PATTERN.format(config_file=config_name), "r") as config_file:
        return json.load(config_file)

def get_num_individuals(dataset_name=DataSetNames.hdgp):
    data_config = get_config(CONFIG_NAME_DATA)
    return data_config[dataset_name]['num_individulas']

def get_num_chrs(dataset_name=DataSetNames.hdgp):
    data_config = get_config(CONFIG_NAME_DATA)
    return data_config[dataset_name]['num_chrs']

def get_sample_sites_file_name(dataset_name=DataSetNames.hdgp):
    data_config = get_config(CONFIG_NAME_DATA)
    return data_config[dataset_name]['sample_sites_file_name']

def get_indlist_file_name(dataset_name=DataSetNames.hdgp):
    data_config = get_config(CONFIG_NAME_DATA)
    return data_config[dataset_name]['indlist_file_name']

def get_dataset_ftp_source_host(dataset_name=DataSetNames.hdgp):
    data_config = get_config(CONFIG_NAME_DATA)
    return data_config[dataset_name]['ftp_source_host']

def get_dataset_ftp_source_path(dataset_name=DataSetNames.hdgp):
    data_config = get_config(CONFIG_NAME_DATA)
    return data_config[dataset_name]['ftp_source_path']

def get_dataset_vcf_files_names(dataset_name=DataSetNames.hdgp):
    data_config = get_config(CONFIG_NAME_DATA)
    return data_config[dataset_name]['vcf_files_names']

def get_dataset_metadata_files_names(dataset_name=DataSetNames.hdgp):
    data_config = get_config(CONFIG_NAME_DATA)
    return data_config[dataset_name]['metadata_files_names']

def get_cluster_data_folder():
    data_config = get_config(CONFIG_NAME_PATHS)
    return data_config['cluster_data_folder']

