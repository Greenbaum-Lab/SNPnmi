# Given a set of vcfs on local machine, submit vcftools jobs to get stats
# TODO - conisder adding a "all_stats" param which will extract all stats, ideally using the cluster.
from os import path
import urllib.request
import time
import sys
import os
from os.path import dirname, abspath
import ftplib
from pathlib import Path
root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)
from utils.vcf_stats_helper import get_vcf_stats, validate_stat_types, StatTypes
from utils.checkpoint_helper import *
from utils.common import get_paths_helper, are_running_submitions
from utils.config import *

SCRIPT_NAME = os.path.basename(__file__)
# python3 get_vcfs_stats.py hgdp freq

def generate_vcfs_stats(options, stat_types):
    dataset_name = options.dataset_name
    paths_helper = get_paths_helper(dataset_name)
    options.vcfs_folder = paths_helper.data_folder
    files_names = get_dataset_vcf_files_names(dataset_name)
    output_folder = paths_helper.vcf_stats_folder

    all_stats_done = True
    for gzvcf_file in files_names:
        # check vcf file exist
        if not path.exists(options.vcfs_folder + gzvcf_file):
            print (f'vcf file is missing {options.vcfs_folder + gzvcf_file}')
            all_stats_done = False
            continue
        options.gzvcf_file = gzvcf_file
        # go over stats (with checkpoint per input file and stat type)
        for stat_type in stat_types:
            options.stat_type = stat_type
            options.output_path_prefix = output_folder + gzvcf_file
            is_executed, msg = execute_with_checkpoint(get_vcf_stats, f'{SCRIPT_NAME}_{gzvcf_file}_{stat_type}', options)
            if is_executed:
                print(f'done - {gzvcf_file} - {stat_type}')
    i = 0
    while are_running_submitions():
        time.sleep(10)
        print(f"Not done yet. Been already {i * 10} seconds")
        i += 1
    return all_stats_done

# wrappers for execution
def get_vcfs_stats(options):
    stat_types = options.args
    assert validate_dataset_name(options.dataset_name)
    assert validate_stat_types(stat_types), f'one of {stat_types} is not included in {",".join(StatTypes)}'
    all_stats_done = generate_vcfs_stats(options, stat_types)
    if all_stats_done:
        # return validate_stats()
        pass
        return True
    return False

def main(options):
    # args should be: [dataset_name, stat_types (comma seperated)]
    s = time.time()
    dataset_name = options.dataset_name
    is_executed, msg = execute_with_checkpoint(get_vcfs_stats, SCRIPT_NAME, options)
    print(f'{msg}. {(time.time()-s)/60} minutes total run time')
    return is_executed

#main([DataSetNames.hdgp_test.value, 'freq'])

if __name__ == "__main__":
    main(sys.argv[1:])