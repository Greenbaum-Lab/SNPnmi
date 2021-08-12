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

from utils.loader import Loader, Timer

root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)
from utils.vcf_stats_helper import get_vcf_stats, validate_stat_types, StatTypes
from utils.checkpoint_helper import *
from utils.common import get_paths_helper, args_parser
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
            print(f'vcf file is missing {options.vcfs_folder + gzvcf_file}')
            all_stats_done = False
            continue
        options.gzvcf_file = gzvcf_file
        # go over stats (with checkpoint per input file and stat type)
        for stat_type in stat_types:
            options.stat_type = stat_type
            options.output_path_prefix = output_folder + gzvcf_file
            with Loader(f"Running {stat_type} "):
                is_executed, msg = execute_with_checkpoint(get_vcf_stats, f'{SCRIPT_NAME}_{gzvcf_file}_{stat_type}', options)
            if is_executed:
                print(f'done - {gzvcf_file} - {stat_type}')
    return all_stats_done

# wrappers for execution
def get_vcfs_stats(options):
    stat_types = options.args
    assert validate_dataset_name(options.dataset_name)
    assert validate_stat_types(stat_types), f'one of {stat_types} is not included in {",".join(StatTypes)}'
    all_stats_done = generate_vcfs_stats(options, stat_types)
    return all_stats_done

def main(options):
    with Timer(f"Stats computing with {options.args}"):
        is_executed, msg = execute_with_checkpoint(get_vcfs_stats, SCRIPT_NAME, options)
        print(msg)
    return is_executed

#main([DataSetNames.hdgp_test.value, 'freq'])

if __name__ == "__main__":
    options = args_parser()
    main(options)
