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

from utils.common import get_paths_helper
from utils.config import get_num_individuals

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
    window_transposed_folder = paths_helper.windows_dir + f'{mac_maf}_{class_name}/transposed/'

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
    folder_012_files = paths_helper.windows_dir + f'{mac_maf}_{class_name}/'

    c = 0
    num_012_files = 0
    total_number_of_sites = 0
    for entry in os.scandir(folder_012_files):
        if entry.name.endswith('.012.tsv.gz'):
            c += 1
            if c%1000==0:
                print(total_number_of_sites)
            print(entry.name)
            num_012_files += 1
            # first index is individual index
            total_number_of_sites += get_num_columns_in_file(entry)-1
    print('total_number_of_sites', total_number_of_sites)

#validate_012_files('mac', 2, 0)


def _file_len(fname):
    i = -1
    with gzip.open(fname,'rb') as f:
        for i, l in enumerate(f):
            pass
    return i + 1

# TODO - we can instead use seek to go to almost the end of the file, and verify that in the last two rows we have what we expect:
# <>,<>
# <>
# need to check that this is indeed faster, and also deal with edge cases (short/empty file)
def _validate_count_dist_file(count_dist_file):
    try:
        # we may have a valid flag for this file
        valid_flag_file = count_dist_file.replace('.tsv.gz', '.valid.flag')
        if os.path.isfile(valid_flag_file):
            return True
        # else, check the length
        # note that if the file doesnt exist, or the structe of it is not good, we will catch it and return false
        if _file_len(count_dist_file) == get_num_individuals()-1:
            # create a flag that this file is valid
            open(valid_flag_file, 'a').close()
            return True
        return False
    except:
        return False

def _validate_count_dist_files(count_dist_files):
    for count_dist_file in count_dist_files:
        if not _validate_count_dist_file(count_dist_file):
            return False, count_dist_file
    return True, None


def max_index_with_n_lines(mac_maf, class_name, n, min_index, max_index):
    paths_helper = get_paths_helper()
    template_012_files = paths_helper.windows_dir + f'{mac_maf}_{class_name}/' + 'window_{i}.012.tsv.gz'
    max_i_found = 0
    valid_is = []
    for i in range(min_index, max_index+1):
        if (_file_len(template_012_files.format(i=i)) == n):
            max_i_found = i
            valid_is.append(i)
    print (valid_is)
    print (max_i_found)

def validate_all_count_dist_files(mac_maf, class_name):
    paths_helper = get_paths_helper()
    assert os.path.isfile(f'{paths_helper.dist_folder}{mac_maf}_{class_name}_all_norm_dist.valid.flag'), f'{paths_helper.dist_folder}{mac_maf}_{class_name}_all_norm_dist.valid.flag'
    assert os.path.isfile(f'{paths_helper.dist_folder}{mac_maf}_{class_name}_all_count_dist.valid.flag'), f'{paths_helper.dist_folder}{mac_maf}_{class_name}_all_count_dist.valid.flag'

def validate_all_classes_all_count_dist():
    for i in range(2,19):
        validate_all_count_dist_files('mac', str(i))
    for i in range(1,50):
        validate_all_count_dist_files('maf', str(i*1.0/100.0))
