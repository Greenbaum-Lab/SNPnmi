import os
import time
from os.path import dirname, abspath, basename
import sys
from random import sample

import numpy as np

from utils.checkpoint_helper import execute_with_checkpoint

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from steps.s5_build_baseline_pst.per_class_sum_n_windows import sum_windows, load_hash_data
from utils.common import get_paths_helper, how_many_jobs_run, validate_stderr_empty, args_parser
from utils.loader import Loader, Timer
from utils.netstrcut_helper import submit_netstruct
from utils.similarity_helper import matrix_to_edges_file

job_type = "mini_net-struct"
SCRIPT_NAME = basename(__file__)


def submit_specific_tree(options, mac_maf, class_val, paths_helper, winds):
    class_name = f'{mac_maf}_{class_val}'
    tree_hash = sum_windows(class_name=class_name, windows_id_list=winds,
                            similarity_window_template=paths_helper.similarity_by_class_and_window_template,
                            count_window_template=paths_helper.count_by_class_and_window_template,
                            output_dir=paths_helper.similarity_by_class_folder_template.format(
                                class_name=class_name),
                            paths_helper=paths_helper)
    job_long_name = f'{class_name}_hash{tree_hash}_ns_{options.ns_ss}_weighted_true'
    job_name = f'ns{class_val}_{tree_hash}'
    similarity_dir = paths_helper.similarity_by_class_folder_template.format(class_name=class_name)
    similarity_matrix_path = similarity_dir + f'{class_name}_hash{tree_hash}_similarity.npy'
    similarity_edges_file = similarity_dir + f'{class_name}_hash{tree_hash}_edges.txt'
    matrix_to_edges_file(similarity_matrix_path, similarity_edges_file)
    output_dir = paths_helper.net_struct_dir + f'{class_name}_{tree_hash}/'
    err_file = submit_netstruct(options, job_type, job_long_name, job_name, similarity_edges_file,
                                output_dir)
    return err_file


def is_tree_valid_and_correct_size(k, v, num_of_winds, class_name, paths_helper):
    if len(v) != num_of_winds:
        return False
    job_long_name = f'{class_name}_hash{k}_weighted_true'
    stderr_file_name = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                             job_name=job_long_name)
    if not os.path.exists(stderr_file_name):
        return False
    if os.stat(stderr_file_name).st_size > 0:
        return False
    net_struct_dir = paths_helper.net_struct_dir
    if not os.path.isdir(f'{net_struct_dir}{class_name}_{k}'):
        return False
    return True


def how_many_tree_computed_before(paths_helper, class_name, num_of_winds):
    hash_file = paths_helper.hash_windows_list_template.format(class_name=class_name)
    data = load_hash_data(hash_file)
    keys_for_hash_in_correct_size = []
    for k, v in data.items():
        if is_tree_valid_and_correct_size(k, v, num_of_winds, class_name, paths_helper):
            keys_for_hash_in_correct_size.append(k)
    return len(keys_for_hash_in_correct_size)


def submit_mini_net_struct_for_class(options, mac_maf, class_val, paths_helper, window_size):
    data_size = options.args[0]
    num_of_trees = options.args[1]
    class_name = f'{mac_maf}_{class_val}'
    num_of_windows_per_tree = data_size / window_size
    assert num_of_windows_per_tree == int(num_of_windows_per_tree), "Data size is not dividable in windows size"
    with open(paths_helper.number_of_windows_per_class_template.format(class_name=class_name), 'r') as f:
        num_of_windows = int(f.read())
        stderr_files = []
    num_computed_trees = how_many_tree_computed_before(paths_helper, class_name, num_of_windows_per_tree)
    rest_num_of_trees = max(0, num_of_trees - num_computed_trees)
    print(f"For class {class_name} there are {num_computed_trees} trees ready. running {rest_num_of_trees} trees to get to {num_of_trees}")
    for tree_idx in range(rest_num_of_trees):
        winds = np.sort(sample(range(num_of_windows), int(num_of_windows_per_tree)))
        stderr_files.append(submit_specific_tree(options, mac_maf, class_val, paths_helper, winds))
    return stderr_files


def submit_mini_net_struct_for_all_classes(options):
    mac_min_range, mac_max_range = options.mac
    maf_min_range, maf_max_range = options.maf
    paths_helper = get_paths_helper(options.dataset_name)
    with open(paths_helper.windows_dir + 'window_size.txt', 'r') as f:
        window_size = int(f.read())
    stderr_files = []

    for mac_maf in ['mac', 'maf']:
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        if min_range >= 0:
            for val in range(min_range, max_range + 1):
                # in maf we take 0.x
                if not is_mac:
                    val = f'{val * 1.0 / 100}'
                stderr_files += submit_mini_net_struct_for_class(options, mac_maf, val, paths_helper, window_size)

    with Loader("Running NetStruct_Hierarchy per many classes", string_to_find='ns'):
        while how_many_jobs_run(string_to_find="ns"):
            time.sleep(5)

    assert validate_stderr_empty(stderr_files)
    return True


def main(options):
    with Timer(f"run net-struct multiple times with {options}"):
        is_executed, msg = execute_with_checkpoint(submit_mini_net_struct_for_all_classes, SCRIPT_NAME, options)
        print(msg)
    return is_executed


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
