

import subprocess
import sys
import os
import time
from os.path import dirname, abspath
import json


root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Loader, Timer
from utils.common import get_paths_helper, args_parser, warp_how_many_jobs, validate_stderr_empty, class_iter
from utils.config import *

SCRIPT_NAME = os.path.basename(__file__)
path_to_python_script_to_run = f'{get_cluster_code_folder()}snpnmi/steps/s4_calc_similarity/calc_similarity_in_window.py'
job_type = 'calc_similarity_windows'


# will submit calc_similarity_in_window of given classes and windows

def submit_calc_similarity_windows(options):

    number_of_windows_to_process_per_job, max_number_of_jobs, initial_window_index = get_args(options)

    # create output folders
    paths_helper = get_paths_helper(options.dataset_name)
    os.makedirs(dirname(paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name='dummy')),
                exist_ok=True)
    os.makedirs(dirname(paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type, job_name='dummy')),
                exist_ok=True)
    number_of_submitted_jobs = 0
    errors = []
    stderr_files = []

    with open(paths_helper.number_of_windows_per_class_path, 'r') as f:
        class2num_windows = json.load(f)
    for cls in class_iter(options):
        os.makedirs(paths_helper.per_window_similarity.format(class_name=cls.name), exist_ok=True)
        if cls.name not in class2num_windows.keys():
            errors.append(cls.name)
            continue
        num_windows = int(class2num_windows[cls.name])
        print(f'{cls.name}, num_windows {num_windows}')
        max_window_id = initial_window_index
        while max_window_id < num_windows:
            min_window_id = max_window_id
            max_window_id = min(min_window_id + number_of_windows_to_process_per_job, num_windows)

            job_long_name = f'fill_{cls.name}_windows{min_window_id}-{max_window_id}'
            job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                    job_name=job_long_name)
            job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                                    job_name=job_long_name)
            stderr_files.append(job_stderr_file)
            job_name = f'c{cls.val}_w{min_window_id}'
            cluster_setting = f'sbatch --time=8:00:00 --error="{job_stderr_file}" --output="{job_stdout_file}' \
                              f'" --job-name="{job_name}"'
            cmd_to_run = f'{cluster_setting} {paths_helper.wrapper_max_30_params} python3 {path_to_python_script_to_run}' \
                         f' -d {options.dataset_name} --args {cls.mac_maf},{cls.int_val},{min_window_id},{max_window_id}'

            subprocess.run([paths_helper.submit_helper, cmd_to_run])
            number_of_submitted_jobs += 1
            if number_of_submitted_jobs == max_number_of_jobs:
                print(f'No more jobs will be submitted. Next window index to process is {max_window_id}')
                break

    if len(errors) == 0:
        print("Done submissions with no errors!")
    else:
        print(f"Errors in:\n{errors}")
    jobs_func = warp_how_many_jobs("_w")
    with Loader("Similarities computations are running", jobs_func):
        while jobs_func():
            time.sleep(5)

    assert validate_stderr_empty(stderr_files)
    return True


def main(options):
    with Timer(f"submit_calc_similarity_windows on {options.args}"):
        submit_calc_similarity_windows(options)
    return True


if __name__ == '__main__':
    run_arguments = args_parser()
    main(run_arguments)


def get_args(options):

    # submission details
    number_of_windows_to_process_per_job = int(options.args[0]) if len(options.args) > 0 else 10 ** 4
    max_number_of_jobs = int(options.args[1]) if len(options.args) > 1 else 1000
    initial_window_index = int(options.args[2]) if len(options.args) > 2 else 0

    # print the inputs
    print('number_of_windows_to_process_per_job', number_of_windows_to_process_per_job)
    print('max_number_of_jobs', max_number_of_jobs)
    print('initial_window_index', initial_window_index)
    return number_of_windows_to_process_per_job, max_number_of_jobs, initial_window_index
