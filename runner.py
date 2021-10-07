

# given a step number and params, will run the step.
import time
import sys
import os
from os.path import dirname, abspath
import argparse
import ftplib
from pathlib import Path

from utils.loader import Timer

root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)
from steps.s1_get_data import get_data, get_vcfs_stats
from steps.s2_split_vcfs_by_class import submit_split_vcfs_by_class, collect_split_vcf_stats
from steps.s3_split_to_windows import submit_prepare_for_split_to_windows
from steps.s3_split_to_windows import submit_split_chr_class_to_windows
from steps.s3_split_to_windows import submit_merge_all_chrs_to_class_windows
from steps.s4_calc_similarity import submit_calc_similarity_windows
from steps.s5_build_baseline_pst import submit_per_class_sum_all_windows,\
    sum_similarities_from_all_classes_and_run_netstrcut, submit_netstruct_per_class, \
    submit_many_netstructs_based_on_fix_size
from steps.s6_compare_to_random_pst import run_nmi

from utils.config import *
from utils.checkpoint_helper import execute_with_checkpoint
from utils.common import args_parser, str_for_timer

step_to_func_and_name = {
    "1.1": (get_data.main, 'get_data'),
    "1.2": (get_vcfs_stats.main, 'get_vcfs_stats'),
    "2.1": (submit_split_vcfs_by_class.main, 'submit_split_vcfs_by_class'),
    "2.2": (collect_split_vcf_stats.main, 'collect_split_vcf_stats'),
    "3.1": (submit_prepare_for_split_to_windows.main, 'submit_prepare_for_split_to_windows'),
    "3.2": (submit_split_chr_class_to_windows.main, 'submit_split_chr_class_to_windows'),
    "3.3": (submit_merge_all_chrs_to_class_windows.main, 'submit_merge_all_chrs_to_class_windows'),
    "4.1": (submit_calc_similarity_windows.main, 'submit_calc_similarity_windows'),
    "5.1": (submit_per_class_sum_all_windows.main, 'submit_per_class_sum_all_windows'),
    "5.2": (sum_similarities_from_all_classes_and_run_netstrcut.main, 'submit_per_class_sum_all_windows'),
    "5.3": (submit_netstruct_per_class.main, 'submit_netstruct_per_class'),
    "5.4": (submit_many_netstructs_based_on_fix_size.main, 'submit_many_netstructs_based_on_fix_size'),
    "6.1": (run_nmi.main, 'run_nmi')
}

def run_step(options, use_checkpoint=True):
    func, step_name = step_to_func_and_name[options.step]
    if not use_checkpoint:
        return func(options)
    # note that we use the step number and name for the checkpont, so this will only not run if we used runner in the past.
    is_executed, msg = execute_with_checkpoint(func, options.step + step_name, options)
    print(msg)
    return is_executed


def runner(options):
    with Timer(f"Runner time with {str_for_timer(options)}"):
        step = options.step
        dataset_name = options.dataset_name
        print(f'Argument List: {step}, {dataset_name}, {str_for_timer(options)}')
        assert validate_dataset_name(dataset_name), f'Invalid dataset name, got: {dataset_name}'

        is_executed = run_step(options)
        print(f'is executed: {is_executed}')

#  python3 runner.py -d hgdp_test -s 1.1

#  python3 runner.py -d hgdp_test -s 1.2 --args freq

#  python3 runner.py -s 2.1 -d hgdp_test --mac 5,8 --maf 46,49

#  python3 runner.py  -s 2.2 -d hgdp_test --mac 5,8 --maf 46,49

#  python3 runner.py -s 3.1 -d hgdp_test --mac 5,8 --maf 46,49 --args 100

#  python3 runner.py -s 3.2 -d hgdp_test --mac 5,8 --maf 46,49

#  python3 runner.py -s 3.3 -d hgdp_test --mac 5,8 --maf 46,49 --args 2000

#  python3 runner.py -s 4.1 -d hgdp_test --mac 5,8 --maf 46,49

#  python3 runner.py -s 5.1 -d hgdp_test --mac 5,8 --maf 46,49

#  python3 runner.py -s 5.2 -d hgdp_test --mac 5,8 --maf 46,49

#  python3 runner.py -s 5.3 -d hgdp_test --mac 5,8 --maf 46,49

#  python3 runner.py -s 5.4 -d hgdp_test --mac 5,8 --maf 46,49 --args 500,7

#  python3 runner.py -s 6.1 -d hgdp_test --mac 5,6 --maf 46,49


if __name__ == "__main__":
    options = args_parser()
    runner(options)
