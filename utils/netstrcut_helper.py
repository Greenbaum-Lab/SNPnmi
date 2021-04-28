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

path_to_wrapper = '/cs/icore/amir.rubin2/code/snpnmi/cluster/wrapper_max_30_params.sh'


def submit_netstcut(job_type, job_long_name, job_name, similarity_matrix_path, output_folder, netstrcut_ss='0.001'):
    # create output folders
    paths_helper = get_paths_helper()
    os.makedirs(dirname(paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name='dummy')), exist_ok=True)
    # job data
    job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name=job_long_name)
    job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type, job_name=job_long_name)
   # cluster setting 
    cluster_setting = f'sbatch --time=72:00:00 --mem=5G --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
   # build netstrcut cmd
    netstruct_cmd = build_netstruct_cmd(similarity_matrix_path, output_folder, netstrcut_ss)
    if netstruct_cmd:
        cmd_to_run=f'{cluster_setting} {path_to_wrapper} {netstruct_cmd}'
        print(cmd_to_run)
        subprocess.run(['/cs/icore/amir.rubin2/code/snpnmi/cluster/submit_helper.sh', cmd_to_run])
        return True



def build_netstruct_cmd(similarity_matrix_path, output_folder, ss='0.001'):
    # validate the input
    if not _validate_count_dist_file(similarity_matrix_path):
        print(f'{similarity_matrix_path} not valid, wont run netstruct')
        return None

    paths_helper = get_paths_helper()
    jar_path = paths_helper.netstruct_jar
    os.makedirs(output_folder, exist_ok=True)

    indlist_path = paths_helper.netstructh_indlist_path
    sample_sites_path = paths_helper.netstructh_sample_sites_path
    return f'java -jar {jar_path} -ss {ss} -minb 5 -mino 5 -pro {output_folder} -pm {similarity_matrix_path} -pmn {indlist_path} -pss {sample_sites_path} -w true'