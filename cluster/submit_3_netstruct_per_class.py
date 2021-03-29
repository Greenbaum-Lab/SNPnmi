# python3 submit_3_netstruct_per_class.py 2 18 1 49 0 499 100

import subprocess
import sys
import os
from os.path import dirname, abspath
root_path = dirname(dirname(os.path.abspath(__file__)))
sys.path.append(root_path)
from utils.common import get_number_of_windows_by_class, get_paths_helper

job_type ='sanity_check_3'
path_to_wrapper = '/cs/icore/amir.rubin2/code/snpnmi/cluster/wrapper_max_30_params.sh'

def submit_3_netstruct_per_class(mac_min_range, mac_max_range, maf_min_range, maf_max_range, min_window_index, max_window_index, max_number_of_jobs):
    # create output folders
    paths_helper = get_paths_helper()
    os.makedirs(dirname(paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name='dummy')), exist_ok=True)
    os.makedirs(dirname(paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type, job_name='dummy')), exist_ok=True)

    number_of_submitted_jobs = 0
    for mac_maf in ['mac', 'maf']:
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        if min_range>0:
            print(f'go over {mac_maf} values: [{min_range},{max_range}]')
            for val in range(min_range, max_range+1):
                if number_of_submitted_jobs == max_number_of_jobs:
                    break
                # in maf we take 0.x
                if not is_mac:
                    val = f'{val * 1.0/100}'
                job_long_name = f'{mac_maf}{val}_{min_window_index}-{max_window_index}'
                job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name=job_long_name)
                job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type, job_name=job_long_name)
                job_name=f's3_{val}'
                cluster_setting=f'sbatch --time=72:00:00 --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
                netstruct_cmd = build_netstructh_cmd(mac_maf, val, min_window_index, max_window_index)
                cmd_to_run=f'{cluster_setting} {path_to_wrapper} {netstruct_cmd}'
                print(cmd_to_run)
                #subprocess.run(['/cs/icore/amir.rubin2/code/snpnmi/cluster/submit_helper.sh', cmd_to_run])
                number_of_submitted_jobs += 1
                if number_of_submitted_jobs == max_number_of_jobs:
                    print(f'No more jobs will be submitted.')
                    break


def build_netstructh_cmd(mac_maf, val, min_window_index, max_window_index):
    paths_helper = get_paths_helper()
    jar_path = paths_helper.netstruct_jar
    output_folder = paths_helper.sanity_check_netstruct_folder
    distances_matrix_path = paths_helper.sanity_check_dist_folder + f'{mac_maf}_{val}_{min_window_index}-{max_window_index}_norm_dist.tsv.gz'
    indlist_path = paths_helper.netstructh_indlist_path
    sample_sites_path = paths_helper.netstructh_sample_sites_path
    return f'java -jar {jar_path} -ss 0.0001 -minb 3 -mino 3 -pro {output_folder} -pm {distances_matrix_path} -pmn {indlist_path} -pss {sample_sites_path}'

if __name__ == '__main__':
    # by mac
    mac_min_range = int(sys.argv[1])
    mac_max_range = int(sys.argv[2])

    # by maf
    maf_min_range = int(sys.argv[3])
    maf_max_range = int(sys.argv[4])

    # submission details
    min_window_index =  int(sys.argv[5])
    max_window_index =  int(sys.argv[6])
    max_number_of_jobs =  int(sys.argv[7])

    # print the inputs
    print('mac_min_range', mac_min_range)
    print('mac_max_range', mac_max_range)
    print('maf_min_range', maf_min_range)
    print('maf_max_range', maf_max_range)
    print('min_window_index', min_window_index)
    print('max_window_index', max_window_index)
    print('max_number_of_jobs', max_number_of_jobs)

    submit_3_netstruct_per_class(mac_min_range, mac_max_range, maf_min_range, maf_max_range, min_window_index, max_window_index, max_number_of_jobs)
