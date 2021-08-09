# python3 submit_per_class_sum_n_windows.py -1 -1 49 49 1000

import subprocess
import sys
import os
from os.path import dirname, abspath


root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)

from utils.config import get_cluster_code_folder
from utils.common import get_number_of_windows_by_class, get_paths_helper, args_parser

job_type = 'per_class_sum_n_windows'
path_to_python_script_to_run = f'{get_cluster_code_folder()}snpnmi/steps/s5_build_baseline_pst/per_class_sum_n_windows.py'


def submit_1_per_class_sum_n_windows(options):
    mac_min_range, mac_max_range, maf_min_range, maf_max_range, num_windows_per_job = get_args(options)
    # create output folders
    paths_helper = get_paths_helper(options.dataset_name)
    os.makedirs(dirname(paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name='dummy')),
                exist_ok=True)
    output_dir = paths_helper.similarity_folder

    for mac_maf in ['mac', 'maf']:
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        if min_range > 0:
            print(f'go over {mac_maf} values: [{min_range},{max_range}]')
            for class_name in range(min_range, max_range + 1):
                # in maf we take 0.x
                if not is_mac:
                    class_name = f'{class_name * 1.0 / 100}'
                class_str = mac_maf + '_' + str(class_name)
                # we log what min and max windows indexes are used so we can consume the files in the next step
                class_dist_files_names_log = f'{output_dir}log_{class_str}_windows_per_job_{num_windows_per_job}.log'
                num_windows = int(get_number_of_windows_by_class(paths_helper)[class_str])
                max_window_id = 0
                # if we have 10 windows, we can only process 0-9
                while max_window_id < num_windows - 1:
                    min_window_id = max_window_id
                    max_window_id = min(min_window_id + num_windows_per_job, num_windows - 1)
                    job_long_name = f'sum_{mac_maf}{class_name}_{min_window_id}-{max_window_id}'
                    job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                            job_name=job_long_name)
                    job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                                            job_name=job_long_name)
                    job_name = f'{class_str[-2:]}_{min_window_id}'
                    cluster_setting = f'sbatch --time=24:00:00 --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
                    python_script_params = f'-d {options.dataset_name} --{mac_maf} {class_name},{min_window_id},{max_window_id}'
                    cmd_to_run = f'{cluster_setting} {paths_helper.wrapper_max_30_params} python3 {path_to_python_script_to_run} {python_script_params}'
                    print(cmd_to_run)
                    subprocess.run([paths_helper.submit_helper, cmd_to_run])
                    with open(class_dist_files_names_log, 'a') as log:
                        log.write(f'{min_window_id}-{max_window_id}\n')

    # TODO : Add validation for the outputs here!


def get_args(options):
    # by mac
    mac_min_range = int(options.mac[0])
    mac_max_range = int(options.mac[1])

    # by maf
    maf_min_range = int(options.maf[0])
    maf_max_range = int(options.maf[1])

    # submission details
    num_windows_per_job = int(options.args[0])
    # print the inputs
    print('mac_min_range', mac_min_range)
    print('mac_max_range', mac_max_range)
    print('maf_min_range', maf_min_range)
    print('maf_max_range', maf_max_range)
    print('num_windows_per_job', num_windows_per_job)

    return mac_min_range, mac_max_range, maf_min_range, maf_max_range, num_windows_per_job


def main(options):
    submit_1_per_class_sum_n_windows(options)
    return True


if __name__ == '__main__':
    arguments = args_parser()
    main(arguments)
