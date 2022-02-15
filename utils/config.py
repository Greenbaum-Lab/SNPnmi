import json
import logging.config
from pathlib import Path
from enum import Enum

import numpy as np

CONFIG_DIR_PATTERN = str(Path(__file__).parents[1]) + '/config/config.{config_file}.json'
CONFIG_NAME_DATA = 'data'
CONFIG_NAME_PATHS = 'paths'

class DataSetNames():
    hdgp = 'hgdp'
    hdgp_test = 'hgdp_test'
    amur = 'amur'
    arabidopsis = "arabidopsis"
    sim_2_v0 = 'sim_2_v0'

def get_config(config_name):
    with open(CONFIG_DIR_PATTERN.format(config_file=config_name), "r") as config_file:
        return json.load(config_file)

def get_num_individuals(dataset_name):
    data_config = get_config(CONFIG_NAME_DATA)
    return data_config[dataset_name]['num_individulas']

def get_num_chrs(dataset_name):
    data_config = get_config(CONFIG_NAME_DATA)
    return data_config[dataset_name]['num_chrs']

def get_min_chr(dataset_name):
    #  Counting on that the vcf_files_short_names are "chrX" with X an int. This is a risky assumption!
    data_config = get_config(CONFIG_NAME_DATA)
    if len(data_config[dataset_name]['vcf_files_short_names']) == 1:
        return 1
    chr_numbers = [int(name[len("chr"):]) for name in data_config[dataset_name]['vcf_files_short_names']]
    return np.min(chr_numbers)

def get_max_chr(dataset_name):
    #  Counting on that the vcf_files_short_names are "chrX" with X an int. This is a risky assumption!
    data_config = get_config(CONFIG_NAME_DATA)
    if len(data_config[dataset_name]['vcf_files_short_names']) == 1:
        return 1
    chr_numbers = [int(name[len("chr"):]) for name in data_config[dataset_name]['vcf_files_short_names']]
    return np.max(chr_numbers)

def get_sample_sites_file_name(dataset_name):
    data_config = get_config(CONFIG_NAME_DATA)
    return data_config[dataset_name]['sample_sites_file_name']

def get_indlist_file_name(dataset_name):
    data_config = get_config(CONFIG_NAME_DATA)
    return data_config[dataset_name]['indlist_file_name']

def get_dataset_ftp_source_host(dataset_name):
    data_config = get_config(CONFIG_NAME_DATA)
    return data_config[dataset_name]['ftp_source_host']

def get_dataset_ftp_source_path(dataset_name):
    data_config = get_config(CONFIG_NAME_DATA)
    return data_config[dataset_name]['ftp_source_path']

def get_dataset_vcf_files_names(dataset_name):
    data_config = get_config(CONFIG_NAME_DATA)
    return data_config[dataset_name]['vcf_files_names']

def get_dataset_vcf_files_short_names(dataset_name):
    data_config = get_config(CONFIG_NAME_DATA)
    return data_config[dataset_name]['vcf_files_short_names']

def get_dataset_metadata_files_names(dataset_name):
    data_config = get_config(CONFIG_NAME_DATA)
    return data_config[dataset_name]['metadata_files_names']

def get_cluster_data_folder():
    data_config = get_config(CONFIG_NAME_PATHS)
    return data_config['cluster_data_folder']

def get_cluster_code_folder():
    data_config = get_config(CONFIG_NAME_PATHS)
    return data_config['cluster_code_folder']

def get_local_code_dir():
    data_config = get_config(CONFIG_NAME_PATHS)
    return data_config['local_code_folder']


# order of vcf_files_short_names should match vcf_files_names.
# we verify each short name is contained in the full name.
def validate_dataset_vcf_files_short_names(dataset_name):
    dataset_vcf_files_short_names = get_dataset_vcf_files_short_names(dataset_name)
    dataset_vcf_files_names = get_dataset_vcf_files_names(dataset_name)
    assert len(dataset_vcf_files_short_names) == len(dataset_vcf_files_names)
