# submit specific windows for mac 2
# python3 submit_calc_similarity_windows.py 2 2 1 8 10 1 -1 -1 -1 True 1014 73511
# python3 submit_calc_similarity_windows.py 2 18 1 1 49 1000 -1 0
# max done so far is: /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/windows/mac_2/count_dist_window_1013.tsv.gz
# max window is: /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/windows/mac_2/window_73511.012.tsv.gz

import subprocess
import sys
import os
import time
from os.path import dirname, abspath
import json


root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Loader, Timer
from utils.common import get_paths_helper, args_parser, are_running_submitions
from utils.config import *

DEFAULT_DELTA_MAC = 1
DEFAULT_DELTA_MAF = 1

SCRIPT_NAME = os.path.basename(__file__)
path_to_python_script_to_run = f'{get_cluster_code_folder()}snpnmi/steps/s4_calc_similarity/calc_similarity_in_window.py'
job_type = 'calc_similarity_windows'


# will submit calc_similarity_in_window of given classes and windows
# python3 submit_calc_similarity_windows.py 3 3 1 80 10 1 1 1 2148
def submit_calc_similarity_windows(options, max_windows_per_job=1000):
    number_of_windows_to_process_per_job, max_number_of_jobs, initial_window_index, mac_min_range, mac_max_range,\
    mac_delta, maf_min_range, maf_max_range, maf_delta, use_specific_012_file, min_input_012_file_index,\
    max_input_012_file_index = get_args(options)

    # create output folders
    paths_helper = get_paths_helper(options.dataset_name)
    os.makedirs(dirname(paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name='dummy')),
                exist_ok=True)
    os.makedirs(dirname(paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type, job_name='dummy')),
                exist_ok=True)


    number_of_submitted_jobs = 0
    errors = []
    if use_specific_012_file:
        print('use_specific_012_file - currently only mac is supported!')
        if mac_min_range > 0:
            print('go over mac values')
            for mac in range(mac_min_range, mac_max_range + 1, mac_delta):
                # init
                job_max_input_012_file_index = min_input_012_file_index - 1
                while job_max_input_012_file_index < max_input_012_file_index:
                    # progress by max_windows_per_job
                    job_min_input_012_file_index = job_max_input_012_file_index + 1
                    # make sure we dont go over the max_input_012_file_index
                    job_max_input_012_file_index = min(job_min_input_012_file_index + max_windows_per_job,
                                                       max_input_012_file_index)

                    if number_of_submitted_jobs == max_number_of_jobs:
                        break
                    job_long_name = f'mac{mac}_012_file{job_min_input_012_file_index}-{job_max_input_012_file_index}'
                    job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                            job_name=job_long_name)
                    job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                                            job_name=job_long_name)
                    job_name = f'c{mac}_{job_min_input_012_file_index}-{job_max_input_012_file_index}'
                    # it takes about 10 minutes to process each window. We have a max of 210 windows. This transalte to 35 hours. using 72 as a buffer.

                    cluster_setting = f'sbatch --time=72:00:00 --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
                    cmd_to_run = f'{cluster_setting} {paths_helper.wrapper_max_30_params} python3 {path_to_python_script_to_run} mac,{mac} -1 -1 -1 -1 {mac} {mac} True {job_min_input_012_file_index} {job_max_input_012_file_index}'
                    print(cmd_to_run)
                    subprocess.run([paths_helper.submit_helper, cmd_to_run])
                    number_of_submitted_jobs += 1
                    if number_of_submitted_jobs == max_number_of_jobs:
                        print(
                            f'No more jobs will be submitted. Last submitted {job_min_input_012_file_index}-{job_max_input_012_file_index}')
                        break
        # if a specific input file is used, we wont go over macs and mafs
        return

    with open(paths_helper.number_of_windows_per_class_path, 'r') as f:
        class2num_windows = json.load(f)
    if mac_min_range > 0:
        print('go over mac values')
        for mac in range(mac_min_range, mac_max_range + 1, mac_delta):
            os.makedirs(paths_helper.per_window_similarity.format(class_name='mac_'+str(mac)), exist_ok=True)
            if f"mac_{mac}" not in class2num_windows.keys():
                errors.append(f"mac_{mac}")
                continue
            num_windows = int(class2num_windows[f"mac_{mac}"])
            print(f'mac {mac}, num_windows {num_windows}')
            max_window_id = initial_window_index
            while max_window_id < num_windows:
                min_window_id = max_window_id
                max_window_id = min(min_window_id + number_of_windows_to_process_per_job, num_windows)
                # go over all windows
                job_long_name = f'fill_mac{mac}_windows{min_window_id}-{max_window_id}'
                job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                        job_name=job_long_name)
                job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                                        job_name=job_long_name)
                job_name = f'c{mac}_w{min_window_id}'
                cluster_setting = f'sbatch --time=2:00:00 --error="{job_stderr_file}" --output="{job_stdout_file}' \
                                  f'" --job-name="{job_name}"'
                cmd_to_run = f'{cluster_setting} {paths_helper.wrapper_max_30_params} python3' \
                             f' {path_to_python_script_to_run} -d {options.dataset_name} --args mac,{mac},' \
                             f'{min_window_id},{max_window_id}'
                print(cmd_to_run)
                subprocess.run([paths_helper.submit_helper, cmd_to_run])
                number_of_submitted_jobs += 1
                if number_of_submitted_jobs == max_number_of_jobs:
                    print(f'No more jobs will be submitted. Next window index to process is {max_window_id}')
                    break

    if maf_min_range > 0:
        print('go over maf values')
        for maf_int in range(maf_min_range, maf_max_range + 1, maf_delta):
            if number_of_submitted_jobs == max_number_of_jobs:
                break
            maf = f'{maf_int * 1.0 / 100}'
            max_maf = f'{(maf_int + maf_delta) * 1.0 / 100}'
            os.makedirs(paths_helper.per_window_similarity.format(class_name='maf_'+str(maf)), exist_ok=True)
            if f"maf_{maf}" not in class2num_windows.keys():
                errors.append(f"maf_{maf}")
                continue
            num_windows = int(class2num_windows[f"maf_{maf}"])
            print(f'maf {maf}, num_windows {num_windows}')
            max_window_id = initial_window_index
            while max_window_id < num_windows:
                min_window_id = max_window_id
                max_window_id = min(min_window_id + number_of_windows_to_process_per_job, num_windows)
                # go over all windows
                job_long_name = f'fill_maf{maf}_windows{min_window_id}-{max_window_id}'
                job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                        job_name=job_long_name)
                job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                                        job_name=job_long_name)
                # to make the jobs name short we only take the last two digits of maf
                job_name = f'f{maf_int}_w{min_window_id}'
                cluster_setting = f'sbatch --time=12:00:00 --error="{job_stderr_file}" --output="{job_stdout_file}"' \
                                  f' --job-name="{job_name}"'
                # maf 0.49 0 0.49 0.5 -1 -1
                cmd_to_run = f'{cluster_setting} {paths_helper.wrapper_max_30_params} python3' \
                             f' {path_to_python_script_to_run} -d {options.dataset_name} --args maf,{maf_int},' \
                             f'{min_window_id},{max_window_id}'
                print(cmd_to_run)
                subprocess.run([paths_helper.submit_helper, cmd_to_run])
                number_of_submitted_jobs += 1
                if number_of_submitted_jobs == max_number_of_jobs:
                    print(f'No more jobs will be submitted. Next window index to process is {max_window_id}')
                    break

    if len(errors) == 0:
        print("Done submissions with no errors!")
    else:
        print(f"Errors in:\n{errors}")
    with Loader("Wait for all similarities computations jobs to be done "):
        while are_running_submitions(string_to_find="_w"):
            time.sleep(5)
    #  TODO: call validate_calc_distances_in_windows


def main(options):
    with Timer(f"submit_calc_similarity_windows on {options.args}"):
        submit_calc_similarity_windows(options)


if __name__ == '__main__':
    run_arguments = args_parser()
    main(run_arguments)


def get_args(options):
    assert 2 <= len(options.mac) <= 3, f"mac argument length is not correct. options.mac = {options.mac}"
    mac_min_range = int(options.mac[0])
    mac_max_range = int(options.mac[1])
    mac_delta = DEFAULT_DELTA_MAC if len(options.mac) == 2 else int(options.mac[2])

    # by maf
    assert 2 <= len(options.maf) <= 3, f"maf argument length is not correct. options.maf = {options.maf}"
    maf_min_range = int(options.maf[0])
    maf_max_range = int(options.maf[1])
    maf_delta = DEFAULT_DELTA_MAF if len(options.maf) == 2 else int(options.maf[2])

    # submission details
    number_of_windows_to_process_per_job = int(options.args[0])
    max_number_of_jobs = int(options.args[1])
    initial_window_index = int(options.args[2])

    # use specific 012 input files
    # when true, for each mac/maf in range, we will process 012 files with index in range [min_input_012_file_index, max_input_012_file_index]
    if hasattr(options, "spec_012"):
        use_specific_012_file = True
        min_input_012_file_index = int(options.spec_012[0])
        max_input_012_file_index = int(options.spec_012[1])
    else:
        use_specific_012_file = False
        min_input_012_file_index = -1
        max_input_012_file_index = -1

    # print the inputs
    print('mac_min_range', mac_min_range)
    print('mac_max_range', mac_max_range)
    print('mac_delta', mac_delta)
    print('maf_min_range', maf_min_range)
    print('maf_max_range', maf_max_range)
    print('maf_delta', maf_delta)
    print('number_of_windows_to_process_per_job', number_of_windows_to_process_per_job)
    print('max_number_of_jobs', max_number_of_jobs)
    print('initial_window_index', initial_window_index)
    print('use_specific_012_file', use_specific_012_file)
    print('min_input_012_file_index', min_input_012_file_index)
    print('max_input_012_file_index', max_input_012_file_index)
    return number_of_windows_to_process_per_job, max_number_of_jobs, initial_window_index, mac_min_range, \
           mac_max_range, mac_delta, maf_min_range, maf_max_range, maf_delta, use_specific_012_file, \
           min_input_012_file_index, max_input_012_file_index
