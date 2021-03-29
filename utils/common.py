import sys
import time

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
    root_data_folder = paths_config['cluster_data_folder'] if is_cluster() else paths_config['local_data_folder']
    root_code_folder = paths_config['cluster_code_folder'] if is_cluster() else paths_config['local_code_folder']

    return PathsHelper(root_data_folder, root_code_folder, data_set_name)


def normalize_distances(distances, counts):
    num_ind = len(distances) + 1
    norm_dists = build_empty_upper_left_matrix(num_ind, 0.0)
    for r_i, (r_dist, r_count) in enumerate(zip(distances, counts)):
        for c_i, (c_dist, c_count) in enumerate(zip(r_dist, r_count)):
            norm_dists[r_i][c_i] = float(c_dist)/float(c_count)
    return norm_dists

class PathsHelper:
    # example: data_folder="/vol/sci/bio/data/gil.greenbaum/amir.rubin/", data_set_name='hgdp'
    def __init__(self, root_data_folder: str, root_code_folder: str, data_set_name: str):
        self.vcf_folder = f'{root_data_folder}vcf/'
        self.data_folder = f'{self.vcf_folder}{data_set_name}/'
        self.classes_folder = f'{self.data_folder}classes/'
        self.windows_folder = f'{self.classes_folder}windows/'
        self.slices_folder = f'{self.classes_folder}slices/'
        self.random_slices_folder = f'{self.classes_folder}random_slices/'

        self.windows_indexes_folder = f'{self.windows_folder}indexes/'
        self.windows_indexes_template = self.windows_indexes_folder + 'windows_indexes_for_class_{class_name}.json'

        self.count_dist_window_template = self.windows_folder + '{mac_maf}_{class_name}/count_dist_window_{window_index}.tsv.gz'

        self.number_of_windows_per_class_path = f'{self.windows_indexes_folder}number_of_windows_per_class.txt'

        self.logs_folder = f'{root_data_folder}logs/'
        self.logs_cluster_folder = f'{self.logs_folder}cluster/'
        self.logs_cluster_jobs_stderr_template = self.logs_cluster_folder + '{job_type}/{job_name}.stderr'
        self.logs_cluster_jobs_stdout_template = self.logs_cluster_folder + '{job_type}/{job_name}.stdout'

        self.split_vcf_stats_csv_path = f'{self.logs_cluster_folder}split_vcfs/split_vcf_output_stats.csv'

        # sanity check folders:
        self.sanity_check_folder = f'{self.classes_folder}sanity_check/'
        self.sanity_check_dist_folder = f'{self.sanity_check_folder}distances/'
        self.sanity_check_netstruct_folder = f'{self.sanity_check_folder}netstruct/'
        self.sanity_check_onmi_folder = f'{self.sanity_check_folder}onmi/'

        # Netstuct inputs paths
        self.netstructh_indlist_path = f'{self.data_folder}indlist.csv'
        self.netstructh_sample_sites_path = f'{self.data_folder}SampleSites.txt'

        # access to code
        self.netstruct_jar = f'{root_code_folder}NetStruct_Hierarchy/NetStruct_Hierarchy_v1.1.jar'



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



def calc_distances_based_on_files(files):
    # use the first file to understand the number of individuals
    with gzip.open(files[0], 'rb') as f:
        num_ind = len(f.readline().split()) + 1
    dists = build_empty_upper_left_matrix(num_ind, 0.0)
    counts = build_empty_upper_left_matrix(num_ind, 0)

    # sum up the distances (and counts) file by file.
    file_i = 0
    print(f'{time.time()}: process file 1/{len(files)}')
    for path in files:
        file_i += 1
        if file_i % 10 == 0:
            print(f'{time.time()}: process file {file_i}/{len(files)}')
        with gzip.open(path, 'rb') as f:
            line = f.readline().decode()
            i = -1
            while line:
                i += 1
                parts = line.replace('\n','').split()
                assert len(parts) == num_ind - 1 - i
                for j, count_dist in enumerate(parts):
                    count, dist = count_dist.split(';', 2)
                    counts[i][j] += int(count)
                    dists[i][j] += float(dist)
                line = f.readline().decode()
            # minus 1 as we only have i to j (without i to i) minus another one as the count is zero based
            assert i == num_ind - 1 - 1
    return dists, counts