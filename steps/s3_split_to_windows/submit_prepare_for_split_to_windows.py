
DEBUG = False
# Per class will submit a job which will generate a file per chr, holding a mapping of sites indexes to windows ids
# such that the windows sizes are window_size or window_size + 1 (across all chrs)
import sys
import time
import os
from os.path import dirname, abspath
root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)
from utils.loader import Loader
from utils.common import get_paths_helper, are_running_submitions
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
        print(window_file)
        if os.path.exists(window_file):
            with open(window_file, 'r') as file:
                windows_per_class[cls] = file.read()

    with open(output_file, 'w') as output:
        json.dump(windows_per_class, output)


def submit_prepare_for_split_to_windows(options):
    dataset_name = options.dataset_name
    mac_min_range, mac_max_range, maf_min_range, maf_max_range, window_size = options.args
    paths_helper = get_paths_helper(dataset_name)
    os.makedirs(paths_helper.windows_folder, exist_ok=True)
    classes = []

    for mac_maf in ['mac', 'maf']:
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        if min_range > 0:
            # Go over mac/maf values
            print(f'go over {mac_maf} values: [{min_range},{max_range}]')
            for class_int_val in range(min_range, max_range+1):
                classes.append(f'{mac_maf}_{class_int_val}')
                print(f'submit for {classes[-1]}')
                job_long_name = generate_job_long_name(mac_maf, class_int_val)
                job_name=f'3p{mac_maf}{class_int_val}'
                python_script_params = f'-d {dataset_name} --args {mac_maf},{class_int_val},{window_size}'
                submit_to_cluster(options, job_type, job_long_name, job_name, path_to_python_script_to_run,
                                  python_script_params, with_checkpoint=False, num_hours_to_run=24, debug=DEBUG)
    with Loader("Wait for all splitting jobs to be done "):
        while are_running_submitions(string_to_find="3pm"):
            time.sleep(5)
    print(f"classes to write: {classes}")
    write_class_to_number_of_windows_file(options, classes)

def main(options):
    s = time.time()
    submit_prepare_for_split_to_windows(options)
    print(f'{(time.time()-s)/60} minutes total run time')
    return True


def _test_me():
    submit_prepare_for_split_to_windows(DataSetNames.hdgp_test, 20, 18, 1, 1, window_size=100)


if DEBUG:
    _test_me()
elif __name__ == '__main__':
    options = args_parser()
    main(options)