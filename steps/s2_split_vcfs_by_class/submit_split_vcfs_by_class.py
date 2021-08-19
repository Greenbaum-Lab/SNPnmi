

DEBUG=False
# Per vcf file, per class, will submit a job (if checkpoint does not exist)
import sys
import time
import os
from os.path import dirname, abspath
root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Loader, Timer
from utils.common import get_paths_helper, are_running_submitions, args_parser, str_for_timer
from utils.config import *
from utils.cluster.cluster_helper import submit_to_cluster
from utils.checkpoint_helper import *
from steps.s2_split_vcfs_by_class.split_vcf_by_class import compute_max_min_val

SCRIPT_NAME = os.path.basename(__file__)
job_type = 'split_vcf_by_class'
path_to_python_script_to_run = f'{get_cluster_code_folder()}snpnmi/steps/s2_split_vcfs_by_class/split_vcf_by_class.py'

def generate_job_long_name(mac_maf, class_val, vcf_file_short_name):
    return f'class_{mac_maf}{class_val}_vcf_{vcf_file_short_name}'

def submit_split_vcfs_by_class(options):
    dataset_name = options.dataset_name
    if len(options.args) == 5:
        mac_min_range, mac_max_range, maf_min_range, maf_max_range, with_checkpoint = options.args
    elif len(options.args) == 4:
        mac_min_range, mac_max_range, maf_min_range, maf_max_range = options.args
        with_checkpoint = True
    else:
        raise TypeError
    # prepare output folders
    paths_helper = get_paths_helper(dataset_name)
    output_dir = paths_helper.classes_folder
    vcfs_dir = paths_helper.data_folder
    vcf_files = get_dataset_vcf_files_names(dataset_name)
    vcf_files_short_names = get_dataset_vcf_files_short_names(dataset_name)
    validate_dataset_vcf_files_short_names(dataset_name)

    for mac_maf in ['mac', 'maf']:
        submit_one_class_split(mac_maf, mac_max_range, mac_min_range, maf_max_range, maf_min_range, options, output_dir,
                               vcf_files, vcf_files_short_names, vcfs_dir, with_checkpoint)
    with Loader("Wait for all splitting jobs to be done "):
        while are_running_submitions(string_to_find="chr"):
            time.sleep(5)


def submit_one_class_split(mac_maf, mac_max_range, mac_min_range, maf_max_range, maf_min_range, options, output_dir,
                           vcf_files, vcf_files_short_names, vcfs_dir, with_checkpoint):
    is_mac = mac_maf == 'mac'
    min_range = mac_min_range if is_mac else maf_min_range
    max_range = mac_max_range if is_mac else maf_max_range
    if min_range > 0:
        # Go over mac/maf values
        print(f'go over {mac_maf} values: [{min_range},{max_range}]')
        for val in range(min_range, max_range + 1):
            if is_output_exits(None, val, mac_maf, output_dir):
                continue
            # go over vcfs
            for (vcf_file, vcf_file_short_name) in zip(vcf_files, vcf_files_short_names):
                if is_output_exits(None, val, mac_maf, output_dir + vcf_file_short_name + '/'):
                    continue
                print(f'submit for {vcf_file_short_name} ({vcf_file})', flush=True)
                vcf_full_path = vcfs_dir + vcf_file
                job_long_name = generate_job_long_name(mac_maf, val, vcf_file_short_name)
                job_name = f'2{val}_{vcf_file_short_name}'
                python_script_params = f'{mac_maf} {val} {vcf_full_path} {vcf_file_short_name} {output_dir}'
                submit_to_cluster(options, job_type, job_long_name, job_name, path_to_python_script_to_run,
                                  python_script_params, with_checkpoint, num_hours_to_run=24, debug=DEBUG)


def is_output_exits(class_max_val, class_min_val, mac_maf, output_dir):
    class_max_val, class_min_val, is_mac = compute_max_min_val(class_max_val, class_min_val, mac_maf)

    output_path = f'{output_dir}{mac_maf}_{class_min_val}'

    # early break if the output file already exists
    output_file = f'{output_path}.012'
    if os.path.exists(output_file):
        print(f'output file already exist. Break. {output_file}')
        return True
    return False



def main(options):
    with Timer(f"Submitting split vcf by class with {str_for_timer(options)}"):
        is_executed, msg = execute_with_checkpoint(submit_split_vcfs_by_class, SCRIPT_NAME, options)
    return is_executed

if __name__ == '__main__':
    arguments = args_parser()
    main(arguments)