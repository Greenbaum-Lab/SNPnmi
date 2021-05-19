

# given a dataset name and step number, will run the step.
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

from utils.config import *


def run_step(dataset_name, step, step_args):
    if step == "1.1":
        return get_data.main(step_args)
    if step == "1.2":
        return get_vcfs_stats.main(step_args)
    if step == "2.1":
        return submit_split_vcfs_by_class.main(step_args)
    if step == "2.2":
        return collect_split_vcf_stats.main(step_args)


def runner(args):
    s = time.time()
    print ('Number of arguments:', len(args), 'arguments.')
    print ('Argument List:', str(args))
    dataset_name = args[0]
    assert validate_dataset_name(dataset_name)
    step = args[1]
    step_args = args[2:]

    print(f'Executing step {step} on dataset {dataset_name} with step args {step_args}.')
    is_executed = run_step(dataset_name, step, step_args)
    print(f'is executed: {is_executed}')

    print(f'{(time.time()-s)/60} minutes total run time')

#runner(['hgdp_test','1.1','hgdp_test'])

#runner(['hgdp_test','1.2','hgdp_test','freq'])

#runner(['hgdp_test','2.1','hgdp_test', 2, 18, 1, 49, True])

runner(['hgdp_test','2.2','hgdp_test', 20, 18, 1, 2])


if __name__ == "__main__X":
   runner(sys.argv[1:])
