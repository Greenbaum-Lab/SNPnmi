import sys
import os
from os.path import dirname, abspath

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)
import subprocess

from utils.common import get_paths_helper
from utils.checkpoint_helper import execute_with_checkpoint

MAX_PARAMS_SUPPORTED = 30


def submit_wrapper(options):
    submit_helper_path, cmd_to_run = options.submit_to_cluster_args
    print(cmd_to_run)
    subprocess.run([submit_helper_path, cmd_to_run])


def submit_to_cluster(options, job_type, job_name, script_path, script_args, job_stdout_file,
                      job_stderr_file, num_hours_to_run=2, memory=4, debug=False):
    # prepare for submission
    paths_helper = get_paths_helper(options.dataset_name)
    os.makedirs(dirname(job_stderr_file), exist_ok=True)

    cluster_setting = f'sbatch --time={num_hours_to_run}:00:00 --mem={memory}G --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
    assert len(script_args.split()) <= MAX_PARAMS_SUPPORTED
    cmd_to_run = f'{cluster_setting} {paths_helper.wrapper_max_30_params} python3 {script_path} {script_args}'
    options.submit_to_cluster_args = [paths_helper.submit_helper, cmd_to_run]
    if debug:
        print(cmd_to_run)
    else:
        execute_with_checkpoint(submit_wrapper, job_type, options)
