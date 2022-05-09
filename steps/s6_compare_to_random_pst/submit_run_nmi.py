#!/sci/labs/gilig/shahar.mazie/icore-data/snpnmi_venv/bin/python3

import sys
import os
from os.path import dirname, basename, abspath
import time


root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)

from utils.cluster.cluster_helper import submit_to_cluster
from utils.config import get_cluster_code_folder
from utils.checkpoint_helper import execute_with_checkpoint
from utils.loader import Timer, Loader
from utils.common import get_paths_helper, args_parser, warp_how_many_jobs, validate_stderr_empty

job_type = 'nmi_per_gt_per_size'
path_to_python_script_to_run = f'{get_cluster_code_folder()}snpnmi/steps/s6_compare_to_random_pst/run_nmi_on_mini_trees.py'
SCRIPT_NAME = basename(__file__)


def get_gt_path_dictionary(options, paths_helper):
    ns_gt_path = ('ns_all', paths_helper.net_struct_dir + f'all/W_1_D_0_Min_5_SS_{options.ns_ss}_B_1.0/')
    sim_gt_path = ('sim_gt', paths_helper.data_dir)
    gt_to_run_with = {ns_gt_path[0]: ns_gt_path[1]}
    if 'sim' in options.dataset_name:
        gt_to_run_with[sim_gt_path[0]] = sim_gt_path[1]
    return gt_to_run_with


def submit_nmi_runs(options):
    paths_helper = get_paths_helper(options.dataset_name)
    gt_paths = get_gt_path_dictionary(options, paths_helper)
    stderr_files = []
    for dsize in options.data_size:
        for gt_name, gt_path in gt_paths.items():
            job_long_name = f'nmi-{dsize}-{gt_name}'
            job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                    job_name=job_long_name)
            job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                                    job_name=job_long_name)
            stderr_files.append(job_stderr_file)
            script_args = f'--args {dsize},{gt_name},{gt_path} -d {options.dataset_name}'
            submit_to_cluster(options, job_type, 's6', path_to_python_script_to_run, script_args, job_stdout_file,
                              job_stderr_file, num_hours_to_run=2, memory=4)

    jobs_func = warp_how_many_jobs("s6")
    with Loader("Computing nmi", jobs_func):
        while jobs_func():
            time.sleep(5)

    assert validate_stderr_empty(stderr_files)
    return True


def main(options):
    with Timer(f"netstruct per class with {options.args}"):
        is_executed, msg = execute_with_checkpoint(submit_nmi_runs, SCRIPT_NAME, options)
        print(msg)
    return is_executed


if __name__ == '__main__':
    arguments = args_parser()
    main(arguments)
