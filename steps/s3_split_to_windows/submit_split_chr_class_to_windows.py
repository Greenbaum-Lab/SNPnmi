
DEBUG = False
# Per char and class will submit a job which will generate for the given class the part of the windows with values from this chr
import sys
import time
import os
from os.path import dirname, abspath

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Loader, Timer
from utils.common import warp_how_many_jobs, args_parser, validate_stderr_empty, class_iter
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
    for chr_name in get_dataset_vcf_files_short_names(dataset_name):
        for cls in class_iter(options):
            job_long_name = generate_job_long_name(cls.mac_maf, cls.int_val, chr_name)
            job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                    job_name=job_long_name)
            job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                                    job_name=job_long_name)
            stderr_files.append(job_stderr_file)
            job_name = f's3_{chr_name[3:]}{cls.mac_maf[-1]}{cls.int_val}'
            memory = 16
            hours = 8
            python_script_params = f'-d {dataset_name} --args {chr_name},{cls.mac_maf},{cls.int_val}'
            submit_to_cluster(options, job_type, job_name, path_to_python_script_to_run,
                              python_script_params, job_stdout_file, job_stderr_file, num_hours_to_run=hours,
                              memory=memory)
    jobs_func = warp_how_many_jobs("s3_")
    with Loader("Splitting jobs are running", jobs_func):
        while jobs_func():
            time.sleep(5)

    assert validate_stderr_empty(stderr_files)

def main(options):
    with Timer(f"split chr class to windows with {options.args}"):
        submit_split_chr_class_to_windows(options)
    return True


if __name__ == '__main__':
    options = args_parser()
    main(options)
