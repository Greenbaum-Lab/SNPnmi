DEBUG = False
# Per class and a number N, will submit jobs which will each merge N windows.
# For example, for class mac 18, and max_num_of_windows_per_job=10, assuming in total the class has 101 (0-100) windows, we will submit 11 jobs:
# 0-9, 10-19, .. , 90-99 and one run with a single window (100).
# it takes about 0.5 seconds per window. A good max_num_of_windows_per_job can be 1000
import sys
import time
from os.path import dirname, abspath

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Loader, Timer
from utils.common import get_paths_helper, AlleleClass, args_parser, how_many_jobs_run, validate_stderr_empty
from utils.config import *
from utils.cluster.cluster_helper import submit_to_cluster
from utils.checkpoint_helper import *

SCRIPT_NAME = os.path.basename(__file__)
job_type = 'merge_all_chrs_to_class_windows'
path_to_python_script_to_run = f'{get_cluster_code_folder()}snpnmi/steps/s3_split_to_windows/merge_all_chrs_to_class_windows.py'


def generate_job_long_name(mac_maf, class_val, min_windows_index, max_windows_index):
    return f'class_{mac_maf}{class_val}_windows_{min_windows_index}_{max_windows_index}'


def get_num_windows_per_class(dataset_name, mac_maf, class_value):
    allele_class = AlleleClass(mac_maf, class_value)
    path_helper = get_paths_helper(dataset_name)
    with open(path_helper.number_of_windows_per_class_template.format(class_name=allele_class.class_name),
              'r') as number_of_windows_per_class_file:
        total_num_of_windows = int(number_of_windows_per_class_file.readline().strip())
        print(f'Class {allele_class.class_name} has {total_num_of_windows} windows')
        return total_num_of_windows


def submit_merge_all_chrs_to_class_windows(options):
    dataset_name = options.dataset_name
    paths_helper = get_paths_helper(dataset_name)
    stderr_files = []
    mac_min_range, mac_max_range = options.mac
    maf_min_range, maf_max_range = options.maf
    max_num_of_windows_per_job = options.args[0] if options.args else 1000
    for mac_maf in ['mac', 'maf']:
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        if min_range > 0:
            # Go over mac/maf values
            print(f'Go over {mac_maf} values: [{min_range},{max_range}]')
            for class_int_val in range(min_range, max_range + 1):
                total_num_windows = get_num_windows_per_class(dataset_name, mac_maf, class_int_val)
                for min_windows_index in range(0, total_num_windows, max_num_of_windows_per_job):
                    max_windows_index = min(total_num_windows, min_windows_index + max_num_of_windows_per_job) - 1
                    print(f'submit for {mac_maf} {class_int_val} windows [{min_windows_index}, {max_windows_index}]')
                    job_long_name = generate_job_long_name(mac_maf, class_int_val, min_windows_index, max_windows_index)
                    job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                            job_name=job_long_name)
                    job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                                            job_name=job_long_name)
                    stderr_files.append(job_stderr_file)
                    job_name = f'3m{mac_maf[-1]}{class_int_val}{min_windows_index}'
                    python_script_params = f'-d {dataset_name} --args {mac_maf},{class_int_val},{min_windows_index},' \
                                           f'{max_windows_index}'
                    submit_to_cluster(options, job_type, job_name, path_to_python_script_to_run, python_script_params,
                                      job_stdout_file, job_stderr_file, num_hours_to_run=2)

    with Loader("Merging jobs are running", string_to_find="3m"):
        while how_many_jobs_run(string_to_find="3m"):
            time.sleep(5)

    assert validate_stderr_empty(stderr_files)


def main(options):
    with Timer(f"merge_all_chrs_to_class_windows on {str_for_timer(options)}"):
        submit_merge_all_chrs_to_class_windows(options)
    return True


if __name__ == '__main__':
    arguments = args_parser()
    main(arguments)
