# NOTE - can only run after 1_per_class_sum_n_windows.py - uses the output it generates!
# given a class and N, we will take N windows from the class and create a distance matrix based on them
# python3 2_per_class_sum_all_windows.py maf 0.40 1000

# takes ~40 seconds for 100 windows.
import pandas as pd
import json
import os
import gzip
import sys
import time
import sys
from os.path import dirname, abspath
root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)
from utils.validate import _validate_count_dist_files, _validate_count_dist_file

from utils.common import get_number_of_windows_by_class, build_empty_upper_left_matrix, write_upper_left_matrix_to_file, get_paths_helper, calc_distances_based_on_files, normalize_distances, write_pairwise_distances

def sum_all_windows(mac_maf, class_name, windows_files, output_dir):

    all_counts_dist_file = f'{output_dir}{mac_maf}_{class_name}_all_count_dist.tsv.gz'
    all_counts_dist_file_validation_flag = f'{output_dir}/{mac_maf}_{class_name}_all_count_dist.valid.flag'

    if os.path.isfile(all_counts_dist_file):
        print(f'all_counts_dist_file exist, do not calc! {all_counts_dist_file}')
        return
    all_norm_distances_file = f'{output_dir}{mac_maf}_{class_name}_all_norm_dist.tsv.gz'
    all_norm_distances_file_validation_flag = f'{output_dir}{mac_maf}_{class_name}_all_norm_dist.valid.flag'

    all_valid, promlematic_file = _validate_count_dist_files(windows_files)

    if not all_valid:
        raise f'promlematic_file: {promlematic_file}'

    dists, counts = calc_distances_based_on_files(windows_files)

    write_pairwise_distances(all_counts_dist_file, counts, dists)
    print(f'all_counts_dist_file : {all_counts_dist_file}')
    if _validate_count_dist_file(all_counts_dist_file):
        # create a flag that this file is valid
        open(all_counts_dist_file_validation_flag, 'a').close()

    norm_distances = normalize_distances(dists, counts)
    write_upper_left_matrix_to_file(all_norm_distances_file, norm_distances)
    print(f'all_norm_distances_file : {all_norm_distances_file}')
    if _validate_count_dist_file(all_norm_distances_file):
        # create a flag that this file is valid
        open(all_norm_distances_file_validation_flag, 'a').close()

def _get_windows_files_names(class_dist_files_names_log, slice_counts_dist_template):
    windows_files = []

    with open(class_dist_files_names_log, 'r') as f:
        lines = f.readlines()
        for line in lines:
            min_index, max_index = line.split('-', 1)
            windows_files.append(slice_counts_dist_template.format(min_window_index=min_index, max_window_index=max_index))
    return windows_files

def main(args):
    s = time.time()
    print ('Number of arguments:', len(args), 'arguments.')
    print ('Argument List:', str(args))
    mac_maf = args[0]
    assert mac_maf=='mac' or mac_maf=='maf'
    class_name = args[1]
    num_windows_per_job = args[2]
    print('mac_maf',mac_maf)
    print('class_name',class_name)

    # Prepare paths
    paths_helper = get_paths_helper()

    output_dir = paths_helper.dist_folder
    class_dist_files_names_log = f'{output_dir}log_{mac_maf}_{class_name}_windows_per_job_{num_windows_per_job}.log'
    slice_counts_dist_template = f'{output_dir}{mac_maf}_{class_name}_' + '{min_window_index}-{max_window_index}_count_dist.tsv.gz'

    windows_files = _get_windows_files_names(class_dist_files_names_log, slice_counts_dist_template)

    print('output_dir',output_dir)

    sum_all_windows(mac_maf, class_name, windows_files, output_dir)

    print(f'{(time.time()-s)/60} minutes total run time')

# mac_maf = 'maf'
# class_name = '0.49'
# num_windows_per_job = '1000'
# main([mac_maf, num_windows_per_job])

if __name__ == "__main__":
   main(sys.argv[1:])
