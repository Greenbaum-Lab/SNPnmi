import sys
import time

from os.path import dirname, abspath
root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)

import os
import gzip

from utils.config import *

from utils.paths_helper import PathsHelper


def get_paths_helper(data_set_name=DataSetNames.hdgp):
    paths_config = get_config(CONFIG_NAME_PATHS)
    root_data_folder = paths_config['cluster_data_folder'] if is_cluster() else paths_config['local_data_folder']
    root_code_folder = paths_config['cluster_code_folder'] if is_cluster() else paths_config['local_code_folder']

    return PathsHelper(root_data_folder, root_code_folder, data_set_name)


def is_cluster():
   return 'cs/icore/amir.rubin2' in os.path.abspath(__file__)


# the output is in couples of <count>;<distance>
# the count is the number of valid sites on which the distances is calculated
def write_pairwise_distances(output_count_dist_file, window_pairwise_counts, window_pairwise_dist):
    with gzip.open(output_count_dist_file,'wb') as f:
        for counts,dists in zip(window_pairwise_counts, window_pairwise_dist):
            s = ' '.join(f'{c};{round(d, 7)}' for c,d in zip(counts, dists)) + '\n'
            f.write(s.encode())


def get_number_of_windows_by_class(number_of_windows_per_class_path=None):
    if not number_of_windows_per_class_path:
        paths_helper = get_paths_helper()
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
