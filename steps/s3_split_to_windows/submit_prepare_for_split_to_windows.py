DEBUG = False
# Per class will submit a job which will generate a file per chr, holding a mapping of sites indexes to windows ids
# such that the windows sizes are window_size or window_size + 1 (across all chrs)
import sys
import time
import os
from os.path import dirname, abspath

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Loader, Timer
from utils.common import get_paths_helper, are_running_submitions, validate_stderr_empty, str_for_timer
from utils.config import *
from utils.cluster.cluster_helper import submit_to_cluster
from utils.checkpoint_helper import *
from utils.common import args_parser

SCRIPT_NAME = os.path.basename(__file__)
job_type = 'prepare_for_split_to_windows'
path_to_python_script_to_run = f'{get_cluster_code_folder()}snpnmi/steps/s3_split_to_windows/prepare_for_split_to_windows.py'


def generate_job_long_name(mac_maf, class_val):
    return f'class_{mac_maf}{class_val}'


def write_class_to_number_of_windows_file(options, classes):
    paths_helper = get_paths_helper(options.dataset_name)
    output_file = paths_helper.number_of_windows_per_class_path
    windows_per_class = {}
    for cls in classes:
        window_file = paths_helper.number_of_windows_per_class_template.format(class_name=cls)
        if os.path.exists(window_file):
            with open(window_file, 'r') as file:
                windows_per_class[cls] = file.read()

    with open(output_file, 'w') as output:
        json.dump(windows_per_class, output)


def submit_prepare_for_split_to_windows(options):
    dataset_name = options.dataset_name
    mac_min_range, mac_max_range = options.mac
    maf_min_range, maf_max_range = options.maf
    window_size = options.args[0]
    paths_helper = get_paths_helper(dataset_name)
    os.makedirs(paths_helper.windows_dir, exist_ok=True)
    classes = []
    stderr_files = []

    for mac_maf in ['mac', 'maf']:
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        if min_range > 0:
            # Go over mac/maf values
            print(f'go over {mac_maf} values: [{min_range},{max_range}]')
            for class_int_val in range(min_range, max_range + 1):
                classes.append(f'{mac_maf}_{class_int_val if mac_maf == "mac" else class_int_val / 100}')
                print(f'submit for {classes[-1]}')
                job_long_name = generate_job_long_name(mac_maf, class_int_val)
                job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                        job_name=job_long_name)
                job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                                        job_name=job_long_name)
                stderr_files.append(job_stderr_file)
                job_name = f'3p{mac_maf}{class_int_val}'
                python_script_params = f'-d {dataset_name} --args {mac_maf},{class_int_val},{window_size}'
                submit_to_cluster(options, job_type, job_name, path_to_python_script_to_run,
                                  python_script_params, job_stdout_file, job_stderr_file, num_hours_to_run=24)

    with Loader("Wait for all splitting jobs to be done "):
        while are_running_submitions(string_to_find="3pm"):
            time.sleep(5)

    write_class_to_number_of_windows_file(options, classes)

    assert validate_stderr_empty(stderr_files)


def main(options):
    with Timer(f"Prepare for split to windows with {str_for_timer(options)}"):
        submit_prepare_for_split_to_windows(options)
    return True


if __name__ == '__main__':
    arguments = args_parser()
    main(arguments)
