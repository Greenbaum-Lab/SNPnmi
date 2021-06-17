

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

from utils.config import *
from utils.checkpoint_helper import execute_with_checkpoint

step_to_func_and_name = {
    "1.1" : (get_data.main, 'get_data'),
    "1.2" : (get_vcfs_stats.main, 'get_vcfs_stats'),
    "2.1" : (submit_split_vcfs_by_class.main, 'submit_split_vcfs_by_class'),
    "2.2" : (collect_split_vcf_stats.main, 'collect_split_vcf_stats'),
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
    print(f'Number of arguments:{2 + len(options.args)} arguments.')
    print(f'Argument List: {options.step}, {options.dataset_name}, {options.args}')
    step = options.step
    step_args = options.args
    # the first arg of the step must be dataset_name
    dataset_name = options.dataset_name
    assert validate_dataset_name(dataset_name), f'First arg of step should be the datasetname, got: {dataset_name}'

    print(f'Executing step {step} with step args {step_args}.')
    is_executed = run_step(options)
    print(f'is executed: {is_executed}')

    print(f'{(time.time()-s)/60} minutes total run time')

# runner(['hgdp_test','1.1','hgdp_test'])

# runner(['hgdp_test','1.2','hgdp_test','freq'])

#runner(['2.1','hgdp_test', 2, 18, 1, 49, True])

# runner(['hgdp_test','2.2','hgdp_test', 20, 18, 1, 2])


def args_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--step", dest="step", help="Step number - see README for further info")
    parser.add_argument("-d", "--dataset_name", dest="dataset_name", help="Name of dataset")
    parser.add_argument("--args", dest="args", help="Any additional args")

    options = parser.parse_args()
    options.args = options.args.split(',') if options.args else []
    options.args = [int(arg) if arg.isdecimal() else arg for arg in options.args]
    return options


if __name__ == "__main__":
    options = args_parser()
    runner(options)
