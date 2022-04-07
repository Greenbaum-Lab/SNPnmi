from tqdm import tqdm

DEBUG = False
# Per class will submit a job which will generate a file per chr, holding a mapping of sites indexes to windows ids
# such that the windows sizes are window_size or window_size + 1 (across all chrs)
import sys
import time
import os
from os.path import dirname, abspath
import subprocess

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Loader, Timer
from utils.common import warp_how_many_jobs, validate_stderr_empty, load_dict_from_json, class_iter
from utils.config import *
from utils.cluster.cluster_helper import submit_to_cluster
from utils.checkpoint_helper import *
from utils.common import args_parser

SCRIPT_NAME = os.path.basename(__file__)
job_type = 'prepare_for_split_to_windows'
path_to_script_to_run = f'{get_cluster_code_folder()}snpnmi/steps/s3_split_to_windows/prepare_for_split_to_windows.py'


def generate_job_long_name(mac_maf, class_val):
    return f'class_{mac_maf}{class_val}'


def write_class_to_number_of_windows_file(options, classes):
    paths_helper = get_paths_helper(options.dataset_name)
    output_file = paths_helper.number_of_windows_per_class_path
    windows_per_class = {}
    for cls in classes:
        window_file = paths_helper.number_of_windows_per_class_template.format(class_name=cls)
        if os.path.exists(window_file):
            with open(window_file, 'r') as file:
                windows_per_class[cls] = file.read()
    old_dict = load_dict_from_json(output_file)
    if any([c in old_dict.keys() for c in classes]):
        assert False, "class was seperated to windows already!"
    new_dict = dict(old_dict, **windows_per_class)  # union dictionaries
    with open(output_file, 'w') as output:
        json.dump(new_dict, output)


def submit_prepare_for_split_to_windows(options):
    dataset_name = options.dataset_name
    window_size = options.args[0]
    paths_helper = get_paths_helper(dataset_name)
    os.makedirs(paths_helper.windows_dir, exist_ok=True)
    os.makedirs(paths_helper.logs_dataset_folder + 'prepare_for_split_to_windows', exist_ok=True)
    window_size_file = paths_helper.windows_dir + 'window_size.txt'
    if not os.path.exists(window_size_file):
        with open(window_size_file, 'w') as f:
            f.write(str(window_size))

    classes = []
    stderr_files = []
    classes_to_run = list(class_iter(options))
    for cls in tqdm(classes_to_run, desc="Preparing split for classes: "):
        classes.append(cls.name)
        job_long_name = generate_job_long_name(cls.mac_maf, cls.int_val)
        job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                job_name=job_long_name)
        job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                                job_name=job_long_name)
        stderr_files.append(job_stderr_file)
        script_params = ['-d', dataset_name, '--args', cls.mac_maf + ',' + str(cls.int_val) + ',' + str(window_size)]
        subprocess.run([path_to_script_to_run] + script_params, stdout=job_stdout_file, stderr=job_stderr_file)

    write_class_to_number_of_windows_file(options, classes)

    assert validate_stderr_empty(stderr_files)


def main(options):
    with Timer(f"Prepare for split to windows with {str_for_timer(options)}"):
        submit_prepare_for_split_to_windows(options)
    return True


if __name__ == '__main__':
    arguments = args_parser()
    main(arguments)
