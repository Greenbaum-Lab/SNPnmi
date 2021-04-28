#python3 submit_2_per_class_sum_all_windows.py -1 -1 49 49 1000

import subprocess
import sys
import os
from os.path import dirname, abspath
root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)
from utils.common import get_number_of_windows_by_class, get_paths_helper

# will submit calc_distances_in_window of given classes and windows
job_type = 'per_class_sum_all_windows'
path_to_python_script_to_run = '/cs/icore/amir.rubin2/code/snpnmi/sanity_check/2_per_class_sum_all_windows.py'


def submit_2_per_class_sum_all_windows(mac_min_range, mac_max_range, maf_min_range, maf_max_range, num_windows_per_job):
    # create output folders
    paths_helper = get_paths_helper()
    os.makedirs(dirname(paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name='dummy')), exist_ok=True)
    output_dir = paths_helper.dist_folder

    for mac_maf in ['mac', 'maf']:
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        if min_range>0:
            print(f'go over {mac_maf} values: [{min_range},{max_range}]')
            for val in range(min_range, max_range+1):
                # in maf we take 0.x
                if not is_mac:
                    val = f'{val * 1.0/100}'

                job_long_name = f'sum_all_{mac_maf}{val}_{num_windows_per_job}'
                job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name=job_long_name)
                job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type, job_name=job_long_name)
                job_name=f's{val}_{num_windows_per_job}'
                cluster_setting=f'sbatch --time=24:00:00 --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
                python_script_params = f'{mac_maf} {val} {num_windows_per_job}'
                cmd_to_run=f'{cluster_setting} {paths_helper.wrapper_max_30_params} python3 {path_to_python_script_to_run} {python_script_params}'
                print(cmd_to_run)
                subprocess.run([paths_helper.submit_helper, cmd_to_run])

if __name__ == '__main__':
    # by mac
    mac_min_range = int(sys.argv[1])
    mac_max_range = int(sys.argv[2])

    # by maf
    maf_min_range = int(sys.argv[3])
    maf_max_range = int(sys.argv[4])

    # submission details
    num_windows_per_job =  int(sys.argv[5])

    # print the inputs
    print('mac_min_range', mac_min_range)
    print('mac_max_range', mac_max_range)
    print('maf_min_range', maf_min_range)
    print('maf_max_range', maf_max_range)
    print('num_windows_per_job', num_windows_per_job)

    submit_2_per_class_sum_all_windows(mac_min_range, mac_max_range, maf_min_range, maf_max_range, num_windows_per_job)
