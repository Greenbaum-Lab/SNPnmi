import gzip
import time
import sys
import os
from os.path import dirname, abspath

root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)
from utils.common import get_paths_helper
from utils.validate import _validate_count_dist_file
import subprocess


def get_cluster_time(ns_ss):
    step_size = float(ns_ss)
    hours = max(int((1/step_size) // 25), 1)
    return f'{hours}:00:00'


def submit_netstruct(options, job_type, job_long_name, job_name, similarity_matrix_path, output_dir):
    # create output folders
    paths_helper = get_paths_helper(options.dataset_name)
    os.makedirs(dirname(paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name='dummy')),
                exist_ok=True)
    # job data
    job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name=job_long_name)
    job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type, job_name=job_long_name)
    # cluster setting
    cluster_time = get_cluster_time(options.ns_ss)
    cluster_setting = f'sbatch --time={cluster_time} --mem=4G --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
    if is_tree_exists(options, output_dir, job_stderr_file):
        print(f"Tree exists already for {job_long_name} with step size {options.ns_ss} - NOT RUNNING!")
        return job_stderr_file

    # build netstrcut cmd
    netstruct_cmd = build_netstruct_cmd(options, similarity_matrix_path, output_dir, options.ns_ss)
    if netstruct_cmd:
        cmd_to_run = f'{cluster_setting} {paths_helper.wrapper_max_30_params} {netstruct_cmd}'
        subprocess.run([paths_helper.submit_helper, cmd_to_run])
    return job_stderr_file


def build_netstruct_cmd(options, similarity_matrix_path, output_folder, ss='0.001'):
    # validate the input
    if not _validate_count_dist_file(options, similarity_matrix_path):
        print(f'{similarity_matrix_path} not valid, wont run netstruct')
        # return None

    paths_helper = get_paths_helper(options.dataset_name)
    jar_path = paths_helper.netstruct_jar
    os.makedirs(output_folder, exist_ok=True)

    indlist_path = paths_helper.netstructh_indlist_path
    sample_sites_path = paths_helper.netstructh_sample_sites_path
    return f'java -jar {jar_path} -ss {ss} -minb 5 -mino 5 -pro {output_folder} -pe {similarity_matrix_path}' \
           f' -pmn {indlist_path} -pss {sample_sites_path} -w true'


def is_tree_exists(options, output_dir, job_err_file):
    if not os.path.exists(output_dir):
        return False
    tree_dirs = os.listdir(output_dir)
    is_correct_tree = [f"SS_{options.ns_ss}" in tree_dir for tree_dir in tree_dirs]
    if sum(is_correct_tree) > 1:
        assert False, "More than 1 tree with the same parameters?"
    if sum(is_correct_tree) < 1:
        return False
    if not os.path.exists(job_err_file) or os.stat(job_err_file).st_size > 0:
        return False
    return True

