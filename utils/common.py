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

        assert isinstance(class_min_int_val,
                          int), f'The class_min_int_val must be an int, even if its maf - we convert it.'
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
            self.class_min_val = (class_min_int_val * 1.0 / 100.0)
            self.class_max_val = (class_max_int_val * 1.0 / 100.0)

        self.class_name = f'{mac_maf}_{self.class_min_val}'


def hash_args(options):
    args = options.args + options.mac + options.maf + [options.ns_ss]
    return hash(tuple(args))


def get_paths_helper(dataset_name):
    paths_config = get_config(CONFIG_NAME_PATHS)
    root_data_folder = paths_config['cluster_data_folder'] if is_cluster() else paths_config['local_data_folder']
    root_code_folder = paths_config['cluster_code_folder'] if is_cluster() else paths_config['local_code_folder']

    return PathsHelper(root_data_folder, root_code_folder, dataset_name)


def is_cluster():
    # danger! make sure local code is not under this path!
    return '/vol/sci/' in os.path.abspath(__file__)



def write_pairwise_similarity(output_similarity_file, similarity_matrix, output_count_file, count_matrix):
    with open(output_similarity_file, 'wb') as f:
        np.save(f, similarity_matrix)
    with open(output_count_file, 'wb') as f:
        np.save(f, count_matrix)


def get_number_of_windows_by_class(paths_helper):
    with open(paths_helper.number_of_windows_per_class_path, 'r') as f:
        class2num_windows = json.load(f)
    return class2num_windows


def build_empty_upper_left_matrix(n, default_value):
    return [[default_value] * (n - i) for i in range(1, n)]


def write_upper_left_matrix_to_file(output_file, values):
    with gzip.open(output_file, 'wb') as f:
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
    raise Exception('Boolean value expected.')


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


def validate_stderr_empty(err_files):
    for err_file in err_files:
        if not os.path.exists(err_file) or os.stat(err_file).st_size > 0:
            print(f"error in file {err_file}\n\n{err_file}")
            return False
    print("Submitting validation - PASS. All stderr files exists and empty.")
    return True


def how_many_jobs_run(string_to_find=""):
    assert is_cluster(), "Cannot check for jobs when run locally"
    username = get_config(CONFIG_NAME_PATHS)["cluster_username"]
    ps = subprocess.Popen(['squeue', '-u', username], stdout=subprocess.PIPE, encoding='utf8')
    try:  # if grep is empty, it raise subprocess.CalledProcessError
        output = subprocess.check_output(('grep', string_to_find), stdin=ps.stdout, encoding='utf8')
        return output.count('\n')
    except subprocess.CalledProcessError:
        return 0


# Deprecated?
def get_class2sites(dataset_name):
    path_helper = get_paths_helper(dataset_name)
    split_vcf_output_stats_file = path_helper.split_vcf_stats_csv_path
    df = pd.read_csv(split_vcf_output_stats_file)
    df['mac_or_maf'] = df.apply(lambda r: r['mac'] if r['mac'] != '-' else r['maf'], axis=1)
    class2sites = dict()
    for c in df['mac_or_maf'].unique():
        print('Prepare indexes for class', c)
        all_class_indexes = []
        for i, r in df[df['mac_or_maf'] == c].iterrows():
            chr_n = r['chr_name_name'][3:]
            num_sites = r['num_of_sites_after_filter']
            all_class_indexes = all_class_indexes + [f'{chr_n};{i}' for i in range(num_sites)]
        print('List is ready, size is:', len(all_class_indexes), '. Shuffle the list')
        random.shuffle(all_class_indexes)
        class2sites[c] = all_class_indexes
        print('Done with class', c)
    return class2sites


def args_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--step", dest="step", help="Step number - see README for further info")
    parser.add_argument("-d", "--dataset_name", dest="dataset_name", help="Name of dataset")
    parser.add_argument("--mac", dest="mac", help="min value, max value, delta")
    parser.add_argument("--override", dest="override", action="store_true", help="If true, can override existing files")
    parser.add_argument("--maf", dest="maf", help="min value, max value, delta")
    parser.add_argument("--args", dest="args", help="Any additional args")
    parser.add_argument("--min_max_allele", dest="min_max_allele", default="2,2", )
    parser.add_argument("--ns_ss", dest="ns_ss", default="0.01",
                        help="Net-struct step size (relevant for step 5 only)")

    options = parser.parse_args()
    options.args = options.args.split(',') if options.args else []
    options.args = [int(i) if i.isdecimal() else i for i in options.args]
    options.mac = options.mac.split(',') if options.mac else []
    options.mac = [int(i) if i.isdecimal() else i for i in options.mac]
    options.maf = options.maf.split(',') if options.maf else []
    options.maf = [int(i) if i.isdecimal() else i for i in options.maf]
    return options


def str_for_timer(options):
    str = ""
    if options.mac:
        str += f"mac--{options.mac}"
    if options.maf:
        str += f"maf--{options.maf}"
    if options.args:
        str += f"args--{options.args}"
    return str

