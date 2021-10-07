# NOTE - can only run after per_class_sum_n_windows.py - uses the output it generates!
# given a class and N, we will take N windows from the class and create a distance matrix based on them
# python3 per_class_sum_all_windows.py maf 0.40 1000

# takes ~40 seconds for 100 windows.
import pandas as pd
import json
import os
import gzip
import sys
import time
import sys
from os.path import dirname, abspath


root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Timer
from utils.common import get_paths_helper, args_parser
from utils.similarity_helper import generate_similarity_matrix


def _get_similarity_per_window_files_names(paths_helper, class_str):
    with open(paths_helper.number_of_windows_per_class_path, 'r') as f:
        num_of_wind_per_class = json.load(f)
    windows_similarity_dir = paths_helper.similarity_by_class_folder_template.format(class_name=class_str) + 'per_window_similarity/'
    count_similarity_files = os.listdir(windows_similarity_dir)
    assert int(num_of_wind_per_class[class_str]) == len(count_similarity_files) / 2  # count and similarity are diff files
    count_files = [windows_similarity_dir + file for file in count_similarity_files if "count" in file]
    count_files.sort()
    similarity_files = [windows_similarity_dir + file for file in count_similarity_files if "similarity" in file]
    similarity_files.sort()
    assert len(similarity_files) == len(count_files)
    assert len([file for file in count_similarity_files if "similarity" in file and "count" in file]) == 0
    assert len(similarity_files) + len(count_files) == len(count_similarity_files)
    return count_files, similarity_files


def main(options):
    print('Number of arguments:', len(options.args), 'arguments.')
    print('Argument List:', str(options.args))
    mac_maf = options.args[0]
    assert mac_maf == 'mac' or mac_maf == 'maf'
    class_name = options.args[1]
    print('mac_maf', mac_maf)
    print('class_name', class_name)
    class_str = f"{mac_maf}_{class_name}"

    # Prepare paths
    paths_helper = get_paths_helper(options.dataset_name)
    output_dir = paths_helper.similarity_by_class_folder_template.format(class_name=class_str)
    count_files, similarity_files = _get_similarity_per_window_files_names(paths_helper, class_str)

    print('output_dir', output_dir)

    generate_similarity_matrix(similarity_files, count_files, output_dir, f'{output_dir}{class_str}_all')


# mac_maf = 'maf'
# class_name = '0.49'
# main([mac_maf])

if __name__ == "__main__":
    arguments = args_parser()
    with Timer(f"per_class_sum_all_windows with {arguments.args}"):
        main(arguments)

