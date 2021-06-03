

# given a step number and params, will run the step.
import time
import sys
import os
from os.path import dirname, abspath
import ftplib
from pathlib import Path
root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)
from steps.s1_get_data import get_data, get_vcfs_stats
from steps.s2_split_vcfs_by_class import submit_split_vcfs_by_class, collect_split_vcf_stats
from steps.s3_split_to_windows import submit_prepare_for_split_to_windows

from utils.config import *
from utils.checkpoint_helper import execute_with_checkpoint

step_to_func_and_name = {
    "1.1" : (get_data.main, 'get_data'),
    "1.2" : (get_vcfs_stats.main, 'get_vcfs_stats'),
    "2.1" : (submit_split_vcfs_by_class.main, 'submit_split_vcfs_by_class'),
    "2.2" : (collect_split_vcf_stats.main, 'collect_split_vcf_stats'),
    "3.1" : (submit_prepare_for_split_to_windows.main, 'submit_prepare_for_split_to_windows'),
}

def run_step(step, dataset_name, step_args, use_checkpoint=True):
    func, step_name = step_to_func_and_name[step]
    if not use_checkpoint:
        return func(step_args)
    # note that we use the step number and name for the checkpont, so this will only not run if we used runner in the past.
    # note that main has a single args = step_args, so we wrap step_args
    is_executed, msg = execute_with_checkpoint(func, step+step_name, dataset_name, [step_args,])
    print(msg)
    return is_executed


def runner(args):
    s = time.time()
    print ('Number of arguments:', len(args), 'arguments.')
    print ('Argument List:', str(args))
    step = args[0]
    step_args = args[1:]
    # the first arg of the step must be dataset_name
    dataset_name = step_args[0]
    assert validate_dataset_name(dataset_name), f'First arg of step should be the datasetname, got: {dataset_name}'

    print(f'Executing step {step} with step args {step_args}.')
    is_executed = run_step(step, dataset_name, step_args)
    print(f'is executed: {is_executed}')

    print(f'{(time.time()-s)/60} minutes total run time')

#runner(['1.1','hgdp_test'])

#runner(['1.2','hgdp_test','freq'])

#runner(['2.1','hgdp_test', 2, 18, 1, 49, True])

#runner(['2.2','hgdp_test', 20, 18, 1, 2])

#runner(['3.1','hgdp_test', 20, 18, 1, 1, 100])


# if __name__ == "__main__":
# # optional - use argparse.ArgumentParser()
#    runner(sys.argv[1:])
