

# given a step number and params, will run the step.
import time
import sys
import os
from os.path import dirname, abspath
import argparse
import ftplib
from pathlib import Path
root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)
from steps.s1_get_data import get_data, get_vcfs_stats
from steps.s2_split_vcfs_by_class import submit_split_vcfs_by_class, collect_split_vcf_stats
from steps.s3_split_to_windows import submit_prepare_for_split_to_windows
from steps.s3_split_to_windows import submit_split_chr_class_to_windows
from steps.s3_split_to_windows import submit_merge_all_chrs_to_class_windows
from steps.s4_calc_similarity import submit_calc_similarity_windows

from utils.config import *
from utils.checkpoint_helper import execute_with_checkpoint
from utils.common import args_parser

step_to_func_and_name = {
    "1.1": (get_data.main, 'get_data'),
    "1.2": (get_vcfs_stats.main, 'get_vcfs_stats'),
    "2.1": (submit_split_vcfs_by_class.main, 'submit_split_vcfs_by_class'),
    "2.2": (collect_split_vcf_stats.main, 'collect_split_vcf_stats'),
    "3.1": (submit_prepare_for_split_to_windows.main, 'submit_prepare_for_split_to_windows'),
    "3.2": (submit_split_chr_class_to_windows.main, 'submit_split_chr_class_to_windows'),
    "3.3": (submit_merge_all_chrs_to_class_windows.main, 'submit_merge_all_chrs_to_class_windows'),
    "4.1": (submit_calc_similarity_windows.main, 'submit_calc_similarity_windows')
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
    s = time.time()
    step = options.step
    step_args = options.args
    dataset_name = options.dataset_name
    print(f'Number of arguments:{2 + len(step_args)} arguments.')
    print(f'Argument List: {step}, {dataset_name}, {step_args}')
    assert validate_dataset_name(dataset_name), f'First arg of step should be the datasetname, got: {dataset_name}'

    print(f'Executing step {step} with step args {step_args}.')
    is_executed = run_step(options)
    print(f'is executed: {is_executed}')

    print(f'{(time.time()-s)/60} minutes total run time')

# runner([-d hgdp_test -s 1.1 --args hgdp_test]))

# runner([-d hgdp_test -s 1.2 --args hgdp_test,freq])

# runner([-s 2.1 -d hgdp_test --args 5,8,46,49,True])

# runner([-d hgdp_test -s 2.2 --args hgdp_test,5,8,46,49])

# runner([-s 3.1 -d hgdp_test --args 5,8,46,49,100])

# runner([-s 3.2 -d hgdp_test --args 5,8,46,49])

# runner([-s 3.3 -d hgdp_test --args 5,8,46,49,100])

# runner([-s 4.1 -d hgdp_test --mac 5,8,1 --maf 46,49,1 --args 100,100,0])

if __name__ == "__main__":
    options = args_parser()
    runner(options)