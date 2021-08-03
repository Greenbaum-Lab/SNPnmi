import pandas as pd
import json
import os
import gzip
import sys
import time
import sys
from os.path import dirname, abspath
import numpy as np

root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)


def get_012_df(input_012_path):
    # get number of columns in chr:
    with gzip.open(input_012_path, 'rb') as f:
        num_columns = len(f.readline().decode().split('\t'))
    # print(f'{len(indexes)} / {num_columns-1} sites will be used from file {class_012_path}')
    names = [f'idx{i}' for i in range(num_columns)]
    # read file to pandas df
    # Legacy: (not true any more) we drop the first column as the csv contains the individual id in the first index
    df = pd.read_csv(input_012_path, sep='\t', names=names, compression='gzip')
    return df


def check_guardrails(mac_maf, max_minor_expected, min_minor_expected, min_valid_sites_percentage, non_ref_count,
                     ref_count, num_of_individuals, num_valid_genotypes):
    assert np.min(num_valid_genotypes / num_of_individuals) > min_valid_sites_percentage
    minor_count = np.min([non_ref_count, ref_count], axis=0)
    if mac_maf == 'mac':
        assert min_minor_expected <= np.min(minor_count) <= np.max(minor_count) <= max_minor_expected
    else:
        minor_ref = minor_count / (2 * num_valid_genotypes)
        assert min_minor_expected <= np.min(minor_ref) <= np.max(minor_ref) <= max_minor_expected
