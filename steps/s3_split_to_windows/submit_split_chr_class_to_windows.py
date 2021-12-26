
DEBUG = False
# Per char and class will submit a job which will generate for the given class the part of the windows with values from this chr
import sys
import time
import os
from os.path import dirname, abspath

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Loader, Timer
from utils.common import get_paths_helper, how_many_jobs_run, args_parser, validate_stderr_empty, is_class_valid
from utils.config import *
from utils.cluster.cluster_helper import submit_to_cluster
from utils.checkpoint_helper import *

SCRIPT_NAME = os.path.basename(__file__)
job_type = 'split_chr_class_to_windows'
path_to_python_script_to_run = f'{get_cluster_code_folder()}snpnmi/steps/s3_split_to_windows/split_chr_class_to_windows.py'


def generate_job_long_name(mac_maf, class_val, chr=""):
    return f'class_{mac_maf}{class_val}{chr}'


def submit_split_chr_class_to_windows(options):
    dataset_name = options.dataset_name
    paths_helper = get_paths_helper(options.dataset_name)
    stderr_files = []
    mac_min_range, mac_max_range = options.mac if options.mac else (1, 0)
    maf_min_range, maf_max_range = options.maf if options.maf else (1, 0)
    chrs_to_run = options.args
    for chr_name in get_dataset_vcf_files_short_names(dataset_name):
        if chrs_to_run:
            if chr_name not in chrs_to_run:
                continue
        for mac_maf in ['mac', 'maf']:
            is_mac = mac_maf == 'mac'
            min_range = mac_min_range if is_mac else maf_min_range
            max_range = mac_max_range if is_mac else maf_max_range
            if min_range > 0:
                # Go over mac/maf values
                print(f'{chr_name} - go over {mac_maf} values: [{min_range},{max_range}]')
                for class_int_val in range(min_range, max_range + 1):
                    if not is_class_valid(options, mac_maf, class_int_val):
                        continue
                    job_long_name = generate_job_long_name(mac_maf, class_int_val, chr_name)
                    job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                            job_name=job_long_name)
                    job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                                            job_name=job_long_name)
                    stderr_files.append(job_stderr_file)
                    job_name = f's3_{chr_name[3:]}{mac_maf[-1]}{class_int_val}'
                    python_script_params = f'-d {dataset_name} --args {chr_name},{mac_maf},{class_int_val}'
                    submit_to_cluster(options, job_type, job_name, path_to_python_script_to_run,
                                      python_script_params, job_stdout_file, job_stderr_file, num_hours_to_run=2,
                                      memory=16)
    with Loader("Splitting jobs are running", string_to_find="s3_"):
        while how_many_jobs_run(string_to_find="s3_"):
            time.sleep(5)

    assert validate_stderr_empty(stderr_files)


def main(options):
    with Timer(f"split chr class to windows with {options.args}"):
        submit_split_chr_class_to_windows(options)
    return True


if __name__ == '__main__':
    options = args_parser()
    main(options)
