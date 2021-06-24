DEBUG = False
# Per class and a number N, will submit jobs which will each merge N windows.
# For example, for class mac 18, and max_num_of_windows_per_job=10, assuming in total the class has 101 (0-100) windows, we will submit 11 jobs:
# 0-9, 10-19, .. , 90-99 and one run with a single window (100).
import sys
import time
import os
from os.path import dirname, abspath
root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)
from utils.common import get_paths_helper, AlleleClass
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
    with open(path_helper.number_of_windows_per_class_template.format(class_name = allele_class.class_name), 'r') as number_of_windows_per_class_file:
        total_num_of_windows = int(number_of_windows_per_class_file.readline().strip())
        print(f'Class {allele_class.class_name} has {total_num_of_windows} windows')
        return total_num_of_windows

def submit_merge_all_chrs_to_class_windows(dataset_name, mac_min_range, mac_max_range, maf_min_range, maf_max_range, max_num_of_windows_per_job):
    paths_helper = get_paths_helper(dataset_name)

    for mac_maf in ['mac', 'maf']:
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        if min_range>0:
            # Go over mac/maf values
            print(f'Go over {mac_maf} values: [{min_range},{max_range}]')
            for class_int_val in range(min_range, max_range+1):
                total_num_windows = get_num_windows_per_class(dataset_name, mac_maf, class_int_val)
                for min_windows_index in range(0, total_num_windows, max_num_of_windows_per_job):
                    max_windows_index = min(total_num_windows ,min_windows_index + max_num_of_windows_per_job) - 1
                    print(f'submit for {mac_maf} {class_int_val} windows [{min_windows_index}, {max_windows_index}]')
                    job_long_name = generate_job_long_name(mac_maf, class_int_val, min_windows_index, max_windows_index)
                    job_name=f'3s{mac_maf}{class_int_val}{min_windows_index}'
                    python_script_params = f'{dataset_name} {mac_maf} {class_int_val} {min_windows_index} {max_windows_index}'
                    submit_to_cluster(dataset_name, job_type, job_long_name, job_name, path_to_python_script_to_run, python_script_params, with_checkpoint=False, num_hours_to_run=24, debug=DEBUG)

def main(args):
    s = time.time()
    dataset_name = args[0]
    submit_merge_all_chrs_to_class_windows(*args)
    print(f'{(time.time()-s)/60} minutes total run time')
    return True

def _test_me():
    submit_merge_all_chrs_to_class_windows(DataSetNames.hdgp_test, 20, 18, 1, 1, 100)

if DEBUG:
    _test_me()
elif __name__ == '__main__':
    main(sys.argv[1:])