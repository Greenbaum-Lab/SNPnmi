import sys
import time
import os
import json
import pandas as pd
import random
import gzip

from os.path import dirname, abspath
root_path = dirname(dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

from utils.common import normalize_distances, get_number_of_windows_by_class, build_empty_upper_left_matrix, write_upper_left_matrix_to_file, str2bool, get_paths_helper
from utils.config import get_num_chrs, get_num_individuals

def get_num_lines_in_file(p):
    with gzip.open(p, 'rb') as f:
        return sum(1 for _ in f)


def get_num_columns_in_file(p):
    with gzip.open(p, 'rb') as f:
        l = f.readline().decode()
        return len(l.split('\t'))


def validate_split_transposed_windows(mac_maf, class_name, min_window_id, max_window_id, expected_number_of_splits_per_window):
    #  we dont want to use the same seed here, as we will have the same values for all clasees
    paths_helper = get_paths_helper()

    # /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/windows/mac_2/transposed/
    window_transposed_folder = paths_helper.windows_folder + f'{mac_maf}_{class_name}/transposed/'

    total_number_of_sites = 0
    for window_id in range(min_window_id, max_window_id+1):

        # output splits
        output_splits_transposed_folder = window_transposed_folder + f'window_{window_id}/'
        output_splits_transposed_template = output_splits_transposed_folder + 'split_{split_id}_transposed.012.tsv.gz'

        # output indexes
        output_indexes_file = paths_helper.windows_indexes_template.format(class_name=f'{class_name}_window_{window_id}_split')
        print(output_indexes_file)
        with open(output_indexes_file, 'r') as jsf:
            split_id_2_indexes = json.load(jsf)
            assert expected_number_of_splits_per_window == len(split_id_2_indexes.keys()), f'found {len(split_id_2_indexes.keys())}'
            for split_id in split_id_2_indexes.keys():
                expected_num_sites_in_slice = len(split_id_2_indexes[split_id])
                total_number_of_sites += expected_num_sites_in_slice
                assert get_num_lines_in_file(output_splits_transposed_template.format(split_id=str(split_id))) == expected_num_sites_in_slice

    print('total_number_of_sites', total_number_of_sites)


def validate_012_files(mac_maf, class_name, expected_number_of_sites):
    #  we dont want to use the same seed here, as we will have the same values for all clasees
    paths_helper = get_paths_helper()

    # /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/windows/mac_2/transposed/
    folder_012_files = paths_helper.windows_folder + f'{mac_maf}_{class_name}/'

    c = 0
    num_012_files = 0
    total_number_of_sites = 0
    for entry in os.scandir(folder_012_files):
        if entry.name.endswith('.012.tsv.gz'):
            c += 1
            if c%1000==0:
                print(c)
            print(entry.name)
            num_012_files += 1
            # first index is individual index
            total_number_of_sites += get_num_columns_in_file(entry)-1
    print('total_number_of_sites', total_number_of_sites)

validate_012_files('mac', 2, 0)