import sys
from os.path import dirname, abspath
root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)

import os
import gzip
from utils.config import *

def is_cluster():
   return 'cs/icore/amir.rubin2' in os.path.abspath(__file__)

def get_paths_helper(data_set_name=DataSetNames.hdgp):
    # from utils.common import get_paths_helper
    # paths_helper = get_paths_helper()
    paths_config = get_config(CONFIG_NAME_PATHS)
    root_folder = paths_config['cluster_root_folder'] if is_cluster() else paths_config['local_root_folder']
    return PathsHelper(root_folder, data_set_name)


def normalize_distances(distances, counts):
    num_ind = len(distances) + 1
    norm_dists = build_empty_upper_left_matrix(num_ind, 0.0)
    for r_i, (r_dist, r_count) in enumerate(zip(distances, counts)):
        for c_i, (c_dist, c_count) in enumerate(zip(r_dist, r_count)):
            norm_dists[r_i][c_i] = float(c_dist)/float(c_count)
    return norm_dists

class PathsHelper:
    # example: root_folder="/vol/sci/bio/data/gil.greenbaum/amir.rubin/", data_set_name='hgdp'
    def __init__(self, root_folder: str, data_set_name: str):
        self.vcf_folder = f'{root_folder}vcf/'
        self.data_folder = f'{self.vcf_folder}{data_set_name}/'
        self.classes_folder = f'{self.data_folder}classes/'
        self.windows_folder = f'{self.classes_folder}windows/'
        self.slices_folder = f'{self.classes_folder}slices/'
        self.random_slices_folder = f'{self.classes_folder}random_slices/'

        self.windows_indexes_folder = f'{self.windows_folder}indexes/'
        self.number_of_windows_per_class_path = f'{self.windows_indexes_folder}number_of_windows_per_class.txt'

        self.logs_folder = f'{root_folder}logs/'
        self.logs_cluster_folder = f'{self.logs_folder}cluster/'
        self.logs_cluster_jobs_stderr_template = self.logs_cluster_folder + '{job_type}/{job_name}.stderr'
        self.logs_cluster_jobs_stdout_template = self.logs_cluster_folder + '{job_type}/{job_name}.stdout'

        self.split_vcf_stats_csv_path = f'{self.logs_cluster_folder}split_vcfs/split_vcf_output_stats.csv'

def get_number_of_windows_by_class(number_of_windows_per_class_path):
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