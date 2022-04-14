
import sys
import os
import time
from os.path import dirname, abspath

from tqdm import tqdm


root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)

from utils.checkpoint_helper import execute_with_checkpoint
from utils.cluster.cluster_helper import submit_to_cluster
from utils.loader import Loader, Timer
from utils.common import get_paths_helper, args_parser, warp_how_many_jobs, validate_stderr_empty, class_iter, \
    str_for_timer
from utils.config import get_cluster_code_folder

SCRIPT_NAME = os.path.basename(__file__)
job_type = 'per_class_sum_all_windows'
path_to_python_script_to_run = f'{get_cluster_code_folder()}snpnmi/steps/s5_build_baseline_pst/per_class_sum_all_windows.py'


def submit_per_class_sum_all_windows(options):
    # create output folders
    paths_helper = get_paths_helper(options.dataset_name)
    os.makedirs(dirname(paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name='dummy')),
                exist_ok=True)
    err_files = []
    for cls in tqdm(list(class_iter(options)), desc="Submitting compute class similarity per class"):
        job_long_name = f'sum_all_{cls.mac_maf}{cls.val}'
        job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                job_name=job_long_name)
        job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                                job_name=job_long_name)
        time_to_run = 24 if (cls.is_mac and cls.val < 5) else 8
        err_files.append(job_stderr_file)
        job_name = f's5_{cls.val}'
        python_script_params = f'-d {options.dataset_name} --args {cls.mac_maf},{cls.val}'
        submit_to_cluster(options, job_type, job_name, path_to_python_script_to_run, python_script_params,
                          job_stdout_file, job_stderr_file, num_hours_to_run=time_to_run)

    jobs_func = warp_how_many_jobs("s5_")
    with Loader(f"Summing all similarity windows per class", jobs_func):
        while jobs_func():
            time.sleep(5)

    assert validate_stderr_empty(err_files)


def main(options):
    with Timer(f"Per class sum all windows on {str_for_timer(options)}"):
        is_success, msg = execute_with_checkpoint(submit_per_class_sum_all_windows, SCRIPT_NAME, options)
    return is_success


if __name__ == '__main__':
    arguments = args_parser()
    main(arguments)
