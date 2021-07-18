from utils.loader import Loader

DEBUG = False
# Per char and class will submit a job which will generate for the given class the part of the windows with values from this chr
import sys
import time
import os
from os.path import dirname, abspath
root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)
from utils.common import get_paths_helper, are_running_submitions
from utils.config import *
from utils.cluster.cluster_helper import submit_to_cluster
from utils.checkpoint_helper import *

SCRIPT_NAME = os.path.basename(__file__)
job_type = 'split_chr_class_to_windows'
path_to_python_script_to_run = f'{get_cluster_code_folder()}snpnmi/steps/s3_split_to_windows/split_chr_class_to_windows.py'

def generate_job_long_name(mac_maf, class_val):
    return f'class_{mac_maf}{class_val}'

def submit_split_chr_class_to_windows(options):
    dataset_name = options.dataset_name
    mac_min_range, mac_max_range, maf_min_range, maf_max_range = options.args
    paths_helper = get_paths_helper(dataset_name)
    for chr_name in get_dataset_vcf_files_short_names(dataset_name):
        for mac_maf in ['mac', 'maf']:
            is_mac = mac_maf == 'mac'
            min_range = mac_min_range if is_mac else maf_min_range
            max_range = mac_max_range if is_mac else maf_max_range
            if min_range>0:
                # Go over mac/maf values
                print(f'{chr_name} - go over {mac_maf} values: [{min_range},{max_range}]')
                for class_int_val in range(min_range, max_range+1):
                    print(f'submit for {chr_name}, {mac_maf} {class_int_val}')
                    job_long_name = generate_job_long_name(mac_maf, class_int_val)
                    job_name=f'3s{chr_name[3:]}{mac_maf}{class_int_val}'
                    python_script_params = f'-d {dataset_name} --args {chr_name},{mac_maf},{class_int_val}'
                    submit_to_cluster(options, job_type, job_long_name, job_name, path_to_python_script_to_run,
                                      python_script_params, with_checkpoint=False, num_hours_to_run=24, debug=DEBUG)
    with Loader("Wait for all splitting jobs to be done "):
        while are_running_submitions(string_to_find="3s"):
            time.sleep(5)


def main(options):
    s = time.time()
    submit_split_chr_class_to_windows(options)
    print(f'{(time.time()-s)/60} minutes total run time')
    return True

def _test_me():
    submit_split_chr_class_to_windows(DataSetNames.hdgp_test, 20, 18, 1, 1)

if DEBUG:
    _test_me()
elif __name__ == '__main__':
    main(sys.argv[1:])