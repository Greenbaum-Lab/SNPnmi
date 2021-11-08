# given a class and N, we will take N windows from the class and create a distance matrix based on them
# python3 per_class_sum_n_windows.py mac 18 0 100

# takes ~40 seconds for 100 windows.
# 6 minutes for 500 windows.
# 10k windows will take 2 hours
import json
import os
import time
import sys
from os.path import dirname, abspath

import numpy as np


root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Timer
from utils.filelock import FileLock
from utils.common import get_paths_helper, args_parser
from utils.similarity_helper import generate_similarity_matrix


def sum_windows(class_name, windows_id_list, similarity_window_template, count_window_template, output_dir,
                paths_helper):
    similarity_files = [similarity_window_template.format(window_id=index, class_name=class_name) for index in
                        windows_id_list]
    count_files = [count_window_template.format(window_id=index, class_name=class_name) for index in windows_id_list]

    new_hash = handle_hash_file(class_name, paths_helper, [int(wind) for wind in windows_id_list])

    generate_similarity_matrix(similarity_files, count_files, output_dir, f'{output_dir}{class_name}_hash{new_hash}',
                               override=False)

    return new_hash


def load_dict_from_json(json_path):
    with FileLock(json_path):
        if not os.path.exists(json_path) or os.stat(json_path).st_size == 0:
            with open(json_path, "w+") as f:
                f.write("{}")
            return {}
        with open(json_path, "r") as f:
            data = json.load(f)
        return data


def handle_hash_file(class_name, paths_helper, windows_id_list):
    windows_id_list = [int(wind) for wind in windows_id_list]
    hash_file = paths_helper.hash_windows_list_template.format(class_name=class_name)
    data = load_dict_from_json(hash_file)
    with FileLock(hash_file):
        hash_codes = [int(i) for i in data.keys()]
        new_hash = 0 if len(hash_codes) == 0 else 1 + max(hash_codes)
        if windows_id_list not in data.values():
            data[str(new_hash)] = windows_id_list
            with open(hash_file, "w") as f:
                json.dump(data, f)
            return new_hash
        else:
            reverse_dict = {tuple(val): key for (key, val) in data.items()}
            return reverse_dict[tuple(windows_id_list)]

def get_args(options):
    print('Number of arguments:', len(options.args), 'arguments.')
    print('Argument List:', str(options.args))
    if options.mac:
        mac_maf = 'mac'
        class_name = options.mac[0]
        min_window_index = int(options.mac[1])
        max_window_index = int(options.mac[2])
    elif options.maf:
        mac_maf = 'maf'
        class_name = options.maf[0]
        min_window_index = int(options.maf[1])
        max_window_index = int(options.maf[2])
    else:
        assert False, "Invalid input was provided"
    assert options.mac == [] or options.maf == []
    assert 0 <= min_window_index <= max_window_index

    paths_helper = get_paths_helper(options.dataset_name)
    class_str = mac_maf + '_' + str(class_name)
    output_dir = paths_helper.similarity_by_class_folder_template.format(class_name=class_str)
    print('count_similarity_window_template', paths_helper.similarity_by_class_and_window_template)
    print('output_dir', output_dir)

    return mac_maf, min_window_index, max_window_index, output_dir, class_str, paths_helper


def main(options):
    mac_maf, min_window_index, max_window_index, output_dir, class_str, paths_helper = get_args(options)
    with Timer(f"per_class_sum_n_windows with {class_str} {min_window_index}-{max_window_index}"):
        windows_list = [i for i in range(min_window_index, max_window_index + 1, 1)]
        sum_windows(class_str, windows_list, paths_helper.similarity_by_class_and_window_template,
                    paths_helper.count_by_class_and_window_template, output_dir, paths_helper)


if __name__ == "__main__":
    options = args_parser()
    main(options)
