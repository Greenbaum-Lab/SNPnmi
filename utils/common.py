import argparse
import itertools
import random
import subprocess
import sys
import time
import pandas as pd
from os.path import dirname, abspath
root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)

import os
import gzip

from utils.config import *

from utils.paths_helper import PathsHelper

# a class to represent classes of alleles count/frequency (mac/maf)
class AlleleClass:
    def __init__(self, mac_maf, class_min_int_val, class_max_int_val=None):
        assert mac_maf in ['mac', 'maf']
        self.mac_maf = mac_maf
        self.is_mac = mac_maf == 'mac'

        assert isinstance(class_min_int_val, int), f'The class_min_int_val must be an int, even if its maf - we convert it.'
        assert class_min_int_val >= 0, f'The class_min_int_val must be non-negative.'
        # maf must be lower than 50
        assert self.is_mac | class_min_int_val < 50
        # the name of the class value is the int min value, even if its maf
        self.class_int_val_name = class_min_int_val
        # set the max val of the class
        if not class_max_int_val:
            # for mac, we want max_val == min_val [2,2]
            if self.is_mac:
                class_max_int_val = class_min_int_val
            # for maf, we want max_val to be min_val+1 [2, 3) (/100)
            # below we will make sure to exclude maf==0.3 from the interval
            else:
                class_max_int_val = class_min_int_val + 1
        if self.is_mac:
            self.class_min_val = class_min_int_val
            self.class_max_val = class_max_int_val
        # for maf we convert to 0.X
        else:
            self.class_min_val = (class_min_int_val*1.0/100.0)
            self.class_max_val = (class_max_int_val*1.0/100.0)

        self.class_name = f'{mac_maf}_{self.class_min_val}'

def hash_args(args):
    hash_val = 0
    for idx, value in enumerate(args):
        hash_val += hash_str(str(value)) * (256 ** idx)
    return hash_val

def hash_str(s):
    hash_val = 0
    for c in s:
        val = ord(c)*17
        hash_val += val
    return hash_val % 256

def get_paths_helper(dataset_name):
    paths_config = get_config(CONFIG_NAME_PATHS)
    root_data_folder = paths_config['cluster_data_folder'] if is_cluster() else paths_config['local_data_folder']
    root_code_folder = paths_config['cluster_code_folder'] if is_cluster() else paths_config['local_code_folder']

    return PathsHelper(root_data_folder, root_code_folder, dataset_name)


def is_cluster():
    # danger! make sure local code is not under this path!
    return '/vol/sci/' in os.path.abspath(__file__)


# the output is in couples of <count>;<similarity>
# the count is the number of valid sites on which the similarity is calculated
def write_pairwise_similarity(output_count_similarity_file, window_pairwise_counts, window_pairwise_similarity):
    # with open(output_count_similarity_file,'w') as f:
    #     for counts,similarities in zip(window_pairwise_counts, window_pairwise_similarity):
    #         txt = ' '.join(f'{c};{round(s, 7)}' for c,s in zip(counts, similarities)) + '\n'
    #         f.write(txt)
    with gzip.open(output_count_similarity_file,'wb') as f:
        for counts,similarities in zip(window_pairwise_counts, window_pairwise_similarity):
            txt = ' '.join(f'{c};{round(s, 7)}' for c,s in zip(counts, similarities)) + '\n'
            f.write(txt.encode())


def get_number_of_windows_by_class(paths_helper):
    number_of_windows_per_class_path = paths_helper.number_of_windows_per_class_path
    class2num_windows = dict()
    with open(number_of_windows_per_class_path) as f:
        for l in f.readlines():
            classname, num_windows = l.split(' ',1)
            class2num_windows[classname] = int(num_windows)
    return class2num_windows


def build_empty_upper_left_matrix(n, default_value):
    return [[default_value]*(n-i) for i in range (1, n)]


def write_upper_left_matrix_to_file(output_file, values):
    with gzip.open(output_file,'wb') as f:
        for v in values:
            s = ' '.join([str(i) for i in v]) + '\n'
            f.write(s.encode())


def str2bool(v) -> bool:
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    if v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    raise 'Boolean value expected.'


def get_num_lines_in_file(p, gzip=False):
    if gzip:
        with gzip.open(p, 'rb') as f:
            return sum(1 for _ in f)
    else:
        with open(p, 'r') as f:
            return sum(1 for _ in f)


def get_num_columns_in_file(p, sep='\t', gzip=False):
    if gzip:
        with gzip.open(p, 'rb') as f:
            l = f.readline().decode()
            return len(l.split(sep))
    else:
        with open(p, 'r') as f:
            l = f.readline()
            return len(l.split(sep))


def are_running_submitions(username="shahar.m", string_to_find=""):
    if not is_cluster():
        return False
    ps = subprocess.Popen('squeue', stdout=subprocess.PIPE)
    try:  # if grep is empty, it raise subprocess.CalledProcessError
        output = subprocess.check_output(('grep', username), stdin=ps.stdout)
        if string_to_find not in str(output):
            return False
        return True
    except subprocess.CalledProcessError:
        return False


# Deprecated?
def get_class2sites(dataset_name):
    path_helper = get_paths_helper(dataset_name)
    split_vcf_output_stats_file = path_helper.split_vcf_stats_csv_path
    df = pd.read_csv(split_vcf_output_stats_file)
    df['mac_or_maf'] = df.apply(lambda r : r['mac'] if r['mac']!='-' else r['maf'], axis=1)
    class2sites = dict()
    for c in df['mac_or_maf'].unique():
        print('Prepare indexes for class',c)
        all_class_indexes = []
        for i,r in df[df['mac_or_maf']==c].iterrows():
            chr_n = r['chr_name_name'][3:]
            num_sites = r['num_of_sites_after_filter']
            all_class_indexes = all_class_indexes + [f'{chr_n};{i}' for i in range(num_sites)]
        print('List is ready, size is:', len(all_class_indexes),'. Shuffle the list')
        random.shuffle(all_class_indexes)
        class2sites[c] = all_class_indexes
        print('Done with class',c)
    return class2sites


def args_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--step", dest="step", help="Step number - see README for further info")
    parser.add_argument("-d", "--dataset_name", dest="dataset_name", help="Name of dataset")
    parser.add_argument("--mac", dest="mac", help="min value, max value, delta")
    parser.add_argument("--maf", dest="maf", help="min value, max value, delta")
    parser.add_argument("--spec_012", dest="use_specific_012_file",
                        help="if not used, default is to use all 012 files. If used should come with 2 args,"
                             " min 012 file and max 012 file")
    parser.add_argument("--args", dest="args", help="Any additional args")

    options = parser.parse_args()
    options.args = options.args.split(',') if options.args else []
    options.args = [int(arg) if arg.isdecimal() else arg for arg in options.args]
    return options