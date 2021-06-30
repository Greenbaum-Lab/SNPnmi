import itertools
import random
import subprocess
import sys
import time

from os.path import dirname, abspath
root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)

import os
import gzip

from utils.config import *

from utils.paths_helper import PathsHelper

def hash_args(args):
    hash_val  = 0
    for i in args:
        hash_val += hash_str(str(i))
    return hash_val

def hash_str(s):
    hash_val  = 0
    for c in s:
        val = ord(c)*17
        hash_val += val
    return hash_val

def get_paths_helper(dataset_name):
    paths_config = get_config(CONFIG_NAME_PATHS)
    root_data_folder = paths_config['cluster_data_folder'] if is_cluster() else paths_config['local_data_folder']
    root_code_folder = paths_config['cluster_code_folder'] if is_cluster() else paths_config['local_code_folder']

    return PathsHelper(root_data_folder, root_code_folder, dataset_name)


def is_cluster():
    # danger! make sure local code is not under this path!
    return '/vol/sci/' in os.path.abspath(__file__)


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


def are_running_submitions(username="shahar.m"):
    ps = subprocess.Popen('squeue', stdout=subprocess.PIPE)
    try:  # if grep is empty, it raise subprocess.CalledProcessError
        subprocess.check_output(('grep', username), stdin=ps.stdout)
        return True
    except subprocess.CalledProcessError:
        return False


def loading_animation(is_done):
    load_msg = "Don't give up! The program is still running😂😂😂"
    for i in itertools.cycle(range(len(load_msg))):
        if is_done():
            break
        sys.stdout.write('\rloading - ' + load_msg[:i])
        time.sleep(0.1)
        sys.stdout.flush()
        time.sleep(0.1)
    sys.stdout.write('\rDone!     ')
