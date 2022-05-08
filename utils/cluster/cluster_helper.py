import sys
import os
from os.path import dirname, abspath

from utils.config import get_num_individuals

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)
import subprocess

from utils.common import get_paths_helper
from utils.checkpoint_helper import execute_with_checkpoint

MAX_PARAMS_SUPPORTED = 30


def submit_wrapper(options):
    paths_helper = get_paths_helper(dataset_name=options.dataset_name)
    submit_helper_path, cmd_to_run = options.submit_to_cluster_args
    subprocess.run([submit_helper_path, cmd_to_run])
    # with open(paths_helper.garbage, "wb") as garbage_output:
    #     subprocess.run([submit_helper_path, cmd_to_run], stdout=garbage_output)


def submit_to_cluster(options, job_type, job_name, script_path, script_args, job_stdout_file,
                      job_stderr_file, num_hours_to_run=8, memory=8, use_checkpoint=True):
    # prepare for submission
    paths_helper = get_paths_helper(options.dataset_name)
    os.makedirs(dirname(job_stderr_file), exist_ok=True)

    cluster_setting = f'sbatch --time={num_hours_to_run}:00:00 --mem={memory}G --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
    assert len(script_args.split()) <= MAX_PARAMS_SUPPORTED
    wrapper = choose_wrapper(options, paths_helper)
    cmd_to_run = f'{cluster_setting} {wrapper} python3 {script_path} {script_args}'
    options.submit_to_cluster_args = [paths_helper.submit_helper, cmd_to_run]
    if use_checkpoint:
        execute_with_checkpoint(submit_wrapper, job_type, options)
    else:
        submit_wrapper(options)


def submit_to_heavy_lab(script_path, script_args, job_stdout_path, job_stderr_path):
    os.makedirs(dirname(job_stderr_path), exist_ok=True)
    params = script_args.split(' ')
    with open(job_stdout_path, 'w') as stdout_file, open(job_stderr_path, 'w') as stderr_file:
        subprocess.Popen(['python3'] + [script_path] + params, stdout=stdout_file, stderr=stderr_file)

def choose_wrapper(options, paths_helper):
    if get_num_individuals(options.dataset_name) > 1024:
        if options.step == '2.1':
            return paths_helper.wrapper_ulimit_2048
    return paths_helper.wrapper_max_30_params
