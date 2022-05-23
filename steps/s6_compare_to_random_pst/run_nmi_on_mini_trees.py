#!/usr/bin/python3

import os
import sys
from os.path import dirname, abspath, basename

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)
from utils import config
sys.path.insert(0, f'{config.get_config(config.CONFIG_NAME_PATHS)["venv_path"]}lib/python3.7')
sys.path.insert(0, f'{config.get_config(config.CONFIG_NAME_PATHS)["venv_path"]}lib/python3.7/site-packages')

print(f"version : {sys.version}")
print(f"path: {sys.path}")
import numpy as np
print(f"np version: {np.__version__}")
from utils.checkpoint_helper import execute_with_checkpoint
from steps.s5_build_baseline_pst.submit_many_netstructs_based_on_fix_size import get_hashes_for_computed_trees
from steps.s6_compare_to_random_pst.nmi_helper import run_all_types_nmi, check_if_nmi_was_computed,\
    collect_all_nodes_if_needed

from utils.common import get_paths_helper, args_parser, get_window_size, class_iter
from utils.loader import Timer

SCRIPT_NAME = basename(__file__)


def compute_nmi_scores_per_class(options, class_name, paths_helper, num_of_winds):
    gt_name = options.args[1]
    gt_path = options.args[2]
    num_of_desired_trees = options.num_of_trees
    hashes_of_fit_trees = get_hashes_for_computed_trees(options, paths_helper, class_name, num_of_winds)
    num_of_trees = len(hashes_of_fit_trees)
    assert num_of_trees >= num_of_desired_trees, f"There are only {num_of_trees} trees for class {class_name}"
    not_computed_trees = check_if_nmi_was_computed(options, paths_helper, class_name, hashes_of_fit_trees,
                                                   gt_name=gt_name)
    num_of_trees_to_run = max(num_of_desired_trees - (num_of_trees - len(not_computed_trees)), 0)

    os.makedirs(paths_helper.nmi_dir.format(gt_name=gt_name), exist_ok=True)
    gt_leafs_overlap = f'{gt_path}2_Leafs_WithOverlap.txt'
    gt_all_nodes = collect_all_nodes_if_needed(gt_path)
    nmi_output_dir = paths_helper.nmi_class_template.format(gt_name=gt_name, class_name=class_name)

    for hash_tree in not_computed_trees[:num_of_trees_to_run]:
        run_all_types_nmi(gt_all_nodes, gt_leafs_overlap, class_name, nmi_output_dir,
                          paths_helper.net_struct_dir_class.format(class_name=class_name), options, hash_tree)


def run_nmi_on_classes_all_trees(options):
    data_size = options.args[0]
    paths_helper = get_paths_helper(options.dataset_name)
    window_size = get_window_size(paths_helper)
    assert data_size // window_size == data_size / window_size, "data size is not dividable in window size"
    num_of_windows = data_size // window_size
    for cls in class_iter(options):
        compute_nmi_scores_per_class(options, cls.name, paths_helper, num_of_windows)
    return True


def main(options):
    with Timer(f"run nmi with {options}"):
        is_success, msg = execute_with_checkpoint(run_nmi_on_classes_all_trees, SCRIPT_NAME, options)
        print(msg)
    return is_success


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
