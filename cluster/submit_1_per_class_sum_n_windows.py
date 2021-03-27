#python3 submit_1_per_class_sum_n_windows.py 2 2 -1 -1 0 100 1
#python3 submit_1_per_class_sum_n_windows.py 2 18 1 49 0 100 100

import subprocess
import sys
import os
from os.path import dirname, abspath
root_path = dirname(dirname(os.path.abspath(__file__)))
sys.path.append(root_path)
from utils.common import get_number_of_windows_by_class, get_paths_helper

# will submit calc_distances_in_window of given classes and windows
job_type ='sanity_check_1'
path_to_python_script_to_run = '/cs/icore/amir.rubin2/code/snpnmi/sanity_check/1_per_class_sum_n_windows.py'
path_to_wrapper = '/cs/icore/amir.rubin2/code/snpnmi/cluster/wrapper_max_10_params.sh'
# python3 submit_calc_dist_windows.py 2 2 1 100 50 1 -1 -1 -1 True 0 100"
def submit_1_per_class_sum_n_windows(mac_min_range, mac_max_range, maf_min_range, maf_max_range, min_window_index, max_window_index, max_number_of_jobs):
    # create output folders
    paths_helper = get_paths_helper()
    os.makedirs(dirname(paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name='dummy')), exist_ok=True)
    os.makedirs(dirname(paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type, job_name='dummy')), exist_ok=True)

    number_of_submitted_jobs = 0
    for mac_maf in ['mac', 'maf']:
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        if min_range>0:
            print(f'go over {mac_maf} values: [{min_range},{max_range}]')
            for val in range(min_range, max_range+1):
                if number_of_submitted_jobs == max_number_of_jobs:
                    break
                # in maf we take 0.x
                if not is_mac:
                    val = f'{val * 1.0/100}'
                job_long_name = f'{mac_maf}{val}_{min_window_index}-{max_window_index}'
                job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name=job_long_name)
                job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type, job_name=job_long_name)
                job_name=f's1_{val}'
                cluster_setting=f'sbatch --time=48:00:00 --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
                python_script_params = f'{mac_maf} {val} {min_window_index} {max_window_index}'
                cmd_to_run=f'{cluster_setting} {path_to_wrapper} {path_to_python_script_to_run} {python_script_params}'
                print(cmd_to_run)
                subprocess.run(['/cs/icore/amir.rubin2/code/snpnmi/cluster/submit_helper.sh', cmd_to_run])
                number_of_submitted_jobs += 1
                if number_of_submitted_jobs == max_number_of_jobs:
                    print(f'No more jobs will be submitted.')
                    break

if __name__ == '__main__':
    # by mac
    mac_min_range = int(sys.argv[1])
    mac_max_range = int(sys.argv[2])

    # by maf
    maf_min_range = int(sys.argv[3])
    maf_max_range = int(sys.argv[4])

    # submission details
    min_window_index =  int(sys.argv[5])
    max_window_index =  int(sys.argv[6])
    max_number_of_jobs =  int(sys.argv[7])

    # print the inputs
    print('mac_min_range', mac_min_range)
    print('mac_max_range', mac_max_range)
    print('maf_min_range', maf_min_range)
    print('maf_max_range', maf_max_range)
    print('min_window_index', min_window_index)
    print('max_window_index', max_window_index)
    print('max_number_of_jobs', max_number_of_jobs)

    submit_1_per_class_sum_n_windows(mac_min_range, mac_max_range, maf_min_range, maf_max_range, min_window_index, max_window_index, max_number_of_jobs)
