# python3 submit_per_class_sum_all_windows.py -1 -1 49 49 1000

import subprocess
import sys
import os
import time
from os.path import dirname, abspath


root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Loader
from utils.common import get_number_of_windows_by_class, get_paths_helper, args_parser, are_running_submitions
from utils.config import get_cluster_code_folder

# will submit calc_distances_in_window of given classes and windows
job_type = 'per_class_sum_all_windows'
path_to_python_script_to_run = f'{get_cluster_code_folder()}snpnmi/steps/s5_build_baseline_pst/per_class_sum_all_windows.py'


def submit_per_class_sum_all_windows(options):
    mac_min_range, mac_max_range, maf_min_range, maf_max_range = get_args(options)
    # create output folders
    paths_helper = get_paths_helper(options.dataset_name)
    os.makedirs(dirname(paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name='dummy')),
                exist_ok=True)

    for mac_maf in ['mac', 'maf']:
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        if min_range > 0:
            print(f'go over {mac_maf} values: [{min_range},{max_range}]')
            for val in range(min_range, max_range + 1):
                # in maf we take 0.x
                if not is_mac:
                    val = f'{val * 1.0 / 100}'

                job_long_name = f'sum_all_{mac_maf}{val}'
                job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                        job_name=job_long_name)
                job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                                        job_name=job_long_name)
                job_name = f's5_{val}'
                cluster_setting = f'sbatch --time=24:00:00 --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
                python_script_params = f'--args {mac_maf},{val}'
                cmd_to_run = f'{cluster_setting} {paths_helper.wrapper_max_30_params} python3 {path_to_python_script_to_run} {python_script_params}'
                print(cmd_to_run)
                subprocess.run([paths_helper.submit_helper, cmd_to_run])

    with Loader(f"Summing all similarity windows per class"):
        while are_running_submitions(string_to_find="s5_"):
            time.sleep(5)


def get_args(options):
    # by mac
    if options.mac:
        mac_min_range = int(options.mac[0])
        mac_max_range = int(options.mac[1])
    else:
        mac_min_range = 0
        mac_max_range = 0

    # by maf
    if options.maf:
        maf_min_range = int(options.maf[0])
        maf_max_range = int(options.maf[1])
    else:
        maf_min_range = 0
        maf_max_range = 0

    # print the inputs
    print('mac_min_range', mac_min_range)
    print('mac_max_range', mac_max_range)
    print('maf_min_range', maf_min_range)
    print('maf_max_range', maf_max_range)


    return mac_min_range, mac_max_range, maf_min_range, maf_max_range


def main(options):
    submit_per_class_sum_all_windows(options)


if __name__ == '__main__':
    arguments = args_parser()
    main(arguments)
