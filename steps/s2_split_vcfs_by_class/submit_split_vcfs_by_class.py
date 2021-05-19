DEBUG=True
# Per vcf file, per class, will submit a job (if checkpoint does not exist)
import sys
import time
import os
from os.path import dirname, abspath
root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)
from utils.common import get_paths_helper
from utils.config import *
from utils.cluster.cluster_helper import submit_to_cluster
from utils.checkpoint_helper import *

SCRIPT_NAME = os.path.basename(__file__)
job_type = 'split_vcf_by_class'
path_to_python_script_to_run = '/cs/icore/amir.rubin2/code/snpnmi/steps/s2_split_vcfs_by_class/split_vcf_by_class.py'


def submit_split_vcfs_by_class(dataset_name, mac_min_range, mac_max_range, maf_min_range, maf_max_range, with_checkpoint):
    # prepare output folders
    paths_helper = get_paths_helper(dataset_name)
    output_dir = paths_helper.classes_folder
    vcfs_dir = paths_helper.vcf_folder
    vcf_files = get_dataset_vcf_files_names(dataset_name)
    vcf_files_short_names = get_dataset_vcf_files_short_names(dataset_name)
    validate_dataset_vcf_files_short_names(dataset_name)

    for mac_maf in ['mac', 'maf']:
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        if min_range>0:
            # Go over mac/maf values
            print(f'go over {mac_maf} values: [{min_range},{max_range}]')
            for val in range(min_range, max_range+1):                
                # go over vcfs
                for (vcf_file, vcf_file_short_name)  in zip(vcf_files, vcf_files_short_names):
                    print(f'submit for {vcf_file_short_name} ({vcf_file})')
                    vcf_full_path = vcfs_dir + vcf_file
                    job_long_name = f'class_{mac_maf}{val}_vcf_{vcf_file_short_name}'
                    job_name=f'2{val}_{vcf_file_short_name}'
                    python_script_params = f'{mac_maf} {val} {vcf_full_path} {vcf_file_short_name} {output_dir}'
                    submit_to_cluster(dataset_name, job_type, job_long_name, job_name, path_to_python_script_to_run, python_script_params, with_checkpoint, num_hours_to_run=24, debug=DEBUG)

def _test_me():
    submit_split_vcfs_by_class(DataSetNames.hdgp_test, 2, 18, 1, 49, with_checkpoint=True)
#_test_me()

def main(args):
    s = time.time()
    dataset_name = args[0]
    is_executed, msg = execute_with_checkpoint(submit_split_vcfs_by_class, SCRIPT_NAME, dataset_name, args)
    print(f'{msg}. {(time.time()-s)/60} minutes total run time')
    return is_executed

# dataset_name, mac_min_range, mac_max_range, maf_min_range, maf_max_range, with_checkpoint
if __name__ == '__main__':
    main(sys.argv[1:])