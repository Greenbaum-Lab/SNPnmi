# Given a set of vcfs on local machine, submit vcftools jobs to get stats
# TODO - conisder adding a "all_stats" param which will extract all stats, ideally using the cluster.
import subprocess
import time
from os import path
import sys
import os
from os.path import dirname

root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Loader, Timer
from steps.s1_get_data.get_single_vcf_stats import get_vcf_stats, validate_stat_types, StatTypes
from utils.checkpoint_helper import *
from utils.common import get_paths_helper, args_parser, are_running_submitions, validate_stderr_empty
from utils.config import *

job_type = 'get_vcf_stats'
python_script_to_run = f'{get_cluster_code_folder()}snpnmi/steps/s1_get_data/get_single_vcf_stats.py'
SCRIPT_NAME = os.path.basename(__file__)

def job_long_name(stats, vcf_short_name):
    return f"get_{stats}_for_{vcf_short_name}"

def generate_vcfs_stats(options, stat_types):
    dataset_name = options.dataset_name
    paths_helper = get_paths_helper(dataset_name)

    options.vcfs_folder = paths_helper.data_folder
    files_names = get_dataset_vcf_files_names(dataset_name)
    short_names = get_dataset_vcf_files_short_names(dataset_name)
    output_folder = paths_helper.vcf_stats_folder
    stderr_files = []
    errors = []
    all_stats_done = True
    os.makedirs(dirname(paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name='dummy')),
                exist_ok=True)
    for short_name, gzvcf_file in zip(short_names, files_names):
        # check vcf file exist
        if not path.exists(options.vcfs_folder + gzvcf_file):
            print(f'vcf file is missing {options.vcfs_folder + gzvcf_file}')
            errors.append(gzvcf_file)
            continue
        options.gzvcf_file = gzvcf_file
        # go over stats (with checkpoint per input file and stat type)
        for stat_type in stat_types:
            job_long_name = f"get_{stat_type}_for_{short_name}"
            job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                    job_name=job_long_name)
            job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                                    job_name=job_long_name)
            stderr_files.append(job_stderr_file)

            options.stat_type = stat_type
            options.output_path_prefix = output_folder + gzvcf_file
            job_name = f's1{stat_type}_{short_name}'
            cluster_setting = f'sbatch --time=2:00:00 --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
            python_script_params = f'-d {options.dataset_name} --args {gzvcf_file},{stat_type}'
            cmd_to_run = f'{cluster_setting} {paths_helper.wrapper_max_30_params} python3 {python_script_to_run} {python_script_params}'
            print(cmd_to_run)
            subprocess.run([paths_helper.submit_helper, cmd_to_run])

    if len(errors) == 0:
        print("Done submissions with no errors!")
    else:
        print(f"Errors in:\n{errors}")

    with Loader("Computing stats"):
        while are_running_submitions(string_to_find="s1"):
            time.sleep(5)

    assert validate_stderr_empty(stderr_files)


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
