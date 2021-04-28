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

from utils.common import get_paths_helper
from utils.similarity_helper import generate_similarity_matrix

def _get_windows_files_names(class_dist_files_names_log, slice_counts_dist_template):
    windows_files = []

    with open(class_dist_files_names_log, 'r') as f:
        lines = f.readlines()
        for line in lines:
            min_index, max_index = line.strip().split('-', 1)
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

    generate_similarity_matrix(windows_files, output_dir, f'{mac_maf}_{class_name}_all')

    print(f'{(time.time()-s)/60} minutes total run time')

# mac_maf = 'maf'
# class_name = '0.49'
# num_windows_per_job = '1000'
# main([mac_maf, num_windows_per_job])

if __name__ == "__main__":
   main(sys.argv[1:])
