from tqdm import tqdm

import sys
import time
from os.path import dirname, abspath

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Loader, Timer
from utils.common import args_parser, warp_how_many_jobs, validate_stderr_empty, class_iter, Cls
from utils.config import *
from utils.cluster.cluster_helper import submit_to_cluster
from utils.checkpoint_helper import *

SCRIPT_NAME = os.path.basename(__file__)
job_type = 'merge_all_chrs_to_class_windows'
path_to_python_script_to_run = f'{get_cluster_code_folder()}snpnmi/steps/s4_calc_similarity/merge_all_chrs_to_class_windows.py'


def generate_job_long_name(mac_maf, class_val, min_windows_index, max_windows_index):
    return f'class_{mac_maf}{class_val}_windows_{min_windows_index}_{max_windows_index}'


def get_num_windows_per_class(dataset_name, mac_maf, class_value):
    cls = Cls(mac_maf, class_value)
    path_helper = get_paths_helper(dataset_name)
    with open(path_helper.number_of_windows_per_class_template.format(class_name=cls.name),
              'r') as number_of_windows_per_class_file:
        total_num_of_windows = int(number_of_windows_per_class_file.readline().strip())
        return total_num_of_windows


def submit_merge_all_chrs_to_class_windows(options):
    dataset_name = options.dataset_name
    paths_helper = get_paths_helper(dataset_name)
    stderr_files = []
    max_num_of_windows_per_job = options.args[0] if options.args else 1000
    for cls in tqdm(list(class_iter(options)), desc="Submitting merge & similarity per class"):
        similarity_dir = paths_helper.similarity_by_class_folder_template.format(class_name=cls.name)
        os.makedirs(similarity_dir, exist_ok=True)
        per_window = paths_helper.per_window_similarity.format(class_name=cls.name)
        os.makedirs(per_window, exist_ok=True)
        total_num_windows = get_num_windows_per_class(dataset_name, cls.mac_maf, cls.int_val)
        for min_windows_index in range(0, total_num_windows, max_num_of_windows_per_job):
            max_windows_index = min(total_num_windows, min_windows_index + max_num_of_windows_per_job) - 1
            job_long_name = generate_job_long_name(cls.mac_maf, cls.int_val, min_windows_index, max_windows_index)
            job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                    job_name=job_long_name)
            job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                                    job_name=job_long_name)
            stderr_files.append(job_stderr_file)
            job_name = f'3m{cls.mac_maf[-1]}{cls.int_val}{min_windows_index}'
            python_script_params = f'-d {dataset_name} --args {cls.mac_maf},{cls.int_val},{min_windows_index},' \
                                   f'{max_windows_index}'
            submit_to_cluster(options, job_type, job_name, path_to_python_script_to_run, python_script_params,
                              job_stdout_file, job_stderr_file, num_hours_to_run=2, use_checkpoint=False)
    jobs_func = warp_how_many_jobs("3m")
    with Loader("Merging jobs are running", jobs_func):
        while jobs_func():
            time.sleep(5)

    assert validate_stderr_empty(stderr_files)
    return True


def main(options):
    with Timer(f"merge_all_chrs_to_class_windows on {str_for_timer(options)}"):
        is_success, msg = execute_with_checkpoint(submit_merge_all_chrs_to_class_windows, SCRIPT_NAME, options)
    return is_success


if __name__ == '__main__':
    arguments = args_parser()
    main(arguments)
