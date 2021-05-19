import sys
import os
from os.path import dirname, abspath
root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)
import subprocess

from utils.common import get_paths_helper
from utils.checkpoint_helper import execute_with_checkpoint

MAX_PARAMS_SUPPORTED=30

def submit_wrapper(submit_helper_path, cmd_to_run):
    print(cmd_to_run)
    subprocess.run([submit_helper_path, cmd_to_run])

def submit_to_cluster(dataset_name, job_type, job_long_name, job_name, script_path, script_args, with_checkpoint, num_hours_to_run=24, debug=False):
    # prepare for submission
    paths_helper = get_paths_helper(dataset_name)
    job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(dataset_name=dataset_name, job_type=job_type, job_name=job_long_name)
    job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(dataset_name=dataset_name, job_type=job_type, job_name=job_long_name)
    os.makedirs(dirname(job_stderr_file), exist_ok=True)

    cluster_setting=f'sbatch --time={num_hours_to_run}:00:00 --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
    assert len(script_args.split()) <= MAX_PARAMS_SUPPORTED
    cmd_to_run=f'{cluster_setting} {paths_helper.wrapper_max_30_params} python3 {script_path} {script_args}'
    args = [paths_helper.submit_helper, cmd_to_run]
    if debug:
        print(cmd_to_run)
    else:
        execute_with_checkpoint(submit_wrapper, job_type, dataset_name, args)