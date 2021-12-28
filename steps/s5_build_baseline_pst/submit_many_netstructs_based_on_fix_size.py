import os
import time
from os.path import dirname, abspath, basename
import sys
from random import sample
import numpy as np

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)


from utils.checkpoint_helper import execute_with_checkpoint
from utils.cluster.cluster_helper import submit_to_cluster
from utils.config import get_cluster_code_folder
from utils.common import get_paths_helper, how_many_jobs_run, validate_stderr_empty, args_parser, get_window_size, \
    load_dict_from_json, handle_hash_file
from utils.loader import Loader, Timer
from utils.netstrcut_helper import is_tree_exists

job_type = "mini_net-struct"
SCRIPT_NAME = basename(__file__)
path_to_python_script_to_run = f'{get_cluster_code_folder()}snpnmi/steps/s5_build_baseline_pst/compute_similarity_and_run_netstruct.py'


def submit_specific_tree(options, mac_maf, class_val, paths_helper, winds):
    class_name = f'{mac_maf}_{class_val}'
    tree_hash = handle_hash_file(class_name, paths_helper, winds)
    job_long_name = f'{class_name}_hash{tree_hash}_ns_{options.ns_ss}_weighted_true'
    job_name = f'ns{class_val}_{tree_hash}'
    output_dir = paths_helper.net_struct_dir_class.format(class_name=class_name) + f'{class_name}_{tree_hash}/'
    # job data
    job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name=job_long_name)
    job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type, job_name=job_long_name)
    # cluster setting
    script_args = f'-d {options.dataset_name} --args {mac_maf},{class_val},{tree_hash} --ns_ss {options.ns_ss}'
    if is_tree_exists(options, output_dir, job_stderr_file):
        print(f"Tree exists already for {job_long_name} with step size {options.ns_ss} - NOT RUNNING!")
        return job_stderr_file
    submit_to_cluster(options, job_type, job_name, path_to_python_script_to_run, script_args, job_stdout_file,
                      job_stderr_file, num_hours_to_run=4, memory=8, debug=False)
    return job_stderr_file


def is_tree_valid_and_correct_size(options, k, v, num_of_winds, class_name, paths_helper):
    if len(v) != num_of_winds:
        return False
    job_long_name = f'{class_name}_hash{k}_ns_{options.ns_ss}_weighted_true'
    stderr_file_name = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                             job_name=job_long_name)
    if not os.path.exists(stderr_file_name):
        return False
    if os.stat(stderr_file_name).st_size > 0:
        return False
    net_struct_dir = paths_helper.net_struct_dir_class.format(class_name=class_name)
    if not os.path.isdir(f'{net_struct_dir}{class_name}_{k}'):
        return False
    list_trees = os.listdir(f'{net_struct_dir}{class_name}_{k}')
    trees_with_correct_ns_ss = [tree for tree in list_trees if f"SS_{options.ns_ss}" in tree]
    if len(trees_with_correct_ns_ss) == 0:
        return False
    return True


def get_hashes_for_computed_trees(options, paths_helper, class_name, num_of_winds):
    hash_file = paths_helper.hash_windows_list_template.format(class_name=class_name)
    data = load_dict_from_json(hash_file)
    keys_for_hash_in_correct_size = []
    for k, v in data.items():
        if is_tree_valid_and_correct_size(options, k, v, num_of_winds, class_name, paths_helper):
            keys_for_hash_in_correct_size.append(k)
    return keys_for_hash_in_correct_size


def how_many_tree_computed_before(options, paths_helper, class_name, num_of_winds):
    keys_for_hash_in_correct_size = get_hashes_for_computed_trees(options, paths_helper, class_name, num_of_winds)
    return len(keys_for_hash_in_correct_size)


def submit_run_one_job_for_all_class_trees(options, mac_maf, class_val, paths_helper, num_of_trees, num_of_windows,
                                           num_of_windows_per_tree):
    class_name = f'{mac_maf}_{class_val}'
    tree_hashes = []
    stderr_files = []
    for tree_idx in range(num_of_trees):
        time.sleep(0.02)  # To avoid FileLock failures.
        winds = np.sort(sample(range(num_of_windows), int(num_of_windows_per_tree)))
        tree_hash = handle_hash_file(class_name, paths_helper, winds)
        tree_hashes.append(tree_hash)
        job_long_name = f'{class_name}_hash{tree_hash}_ns_{options.ns_ss}_weighted_true'
        job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                job_name=job_long_name)
        stderr_files.append(job_stderr_file)
    script_to_run = f'{get_cluster_code_folder()}snpnmi/steps/s5_build_baseline_pst/run_ns_mini_trees_for_class.py'
    params_to_run = f'-d {options.dataset_name} --args {mac_maf},{class_val},{",".join([str(i) for i in tree_hashes])} ' \
                    f'--ns_ss {options.ns_ss}'
    job_long_name = f'{class_name}_few_trees_ns_{options.ns_ss}_weighted_true'
    job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                            job_name=job_long_name)
    job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                            job_name=job_long_name)

    job_short_name = f'ns_{class_name}'
    submit_to_cluster(options, job_type="step5.4 per class", job_name=job_short_name, script_path=script_to_run,
                      script_args=params_to_run, job_stdout_file=job_stdout_file, job_stderr_file=job_stderr_file)
    return stderr_files



def submit_mini_net_struct_for_class(options, mac_maf, class_val, paths_helper, window_size):
    data_size = options.args[0]
    num_of_trees = options.args[1]

    class_name = f'{mac_maf}_{class_val}'
    num_of_windows_per_tree = data_size / window_size
    assert num_of_windows_per_tree == int(num_of_windows_per_tree), "Data size is not dividable in windows size"
    with open(paths_helper.number_of_windows_per_class_template.format(class_name=class_name), 'r') as f:
        num_of_windows = int(f.read())

    num_computed_trees = how_many_tree_computed_before(options, paths_helper, class_name, num_of_windows_per_tree)
    rest_num_of_trees = max(0, num_of_trees - num_computed_trees)
    print(
        f"For class {class_name} there are {num_computed_trees} trees ready. running {rest_num_of_trees} trees to get to {num_of_trees}")

    if options.run_ns_together and rest_num_of_trees:
        return submit_run_one_job_for_all_class_trees(options, mac_maf, class_val, paths_helper, rest_num_of_trees,
                                                      num_of_windows, num_of_windows_per_tree)
    stderr_files = []
    for tree_idx in range(rest_num_of_trees):
        time.sleep(0.02)  # To avoid FileLock failures.
        winds = np.sort(sample(range(num_of_windows), int(num_of_windows_per_tree)))
        stderr_files.append(submit_specific_tree(options, mac_maf, class_val, paths_helper, winds))
    return stderr_files


def submit_mini_net_struct_for_all_classes(options):
    mac_min_range, mac_max_range = options.mac
    maf_min_range, maf_max_range = options.maf
    paths_helper = get_paths_helper(options.dataset_name)
    window_size = get_window_size(paths_helper)
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
