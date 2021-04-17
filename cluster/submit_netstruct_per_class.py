# python3 submit_netstruct_per_class.py 2 18 1 49 70

import subprocess
import sys
import os
from os.path import dirname, abspath
root_path = dirname(dirname(os.path.abspath(__file__)))
sys.path.append(root_path)
from utils.common import get_number_of_windows_by_class, get_paths_helper

job_type ='netstruct_per_class'
path_to_wrapper = '/cs/icore/amir.rubin2/code/snpnmi/cluster/wrapper_max_30_params.sh'

def submit_netstruct_per_class(mac_min_range, mac_max_range, maf_min_range, maf_max_range, max_number_of_jobs):
    # create output folders
    paths_helper = get_paths_helper()
    os.makedirs(dirname(paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name='dummy')), exist_ok=True)
    os.makedirs(dirname(paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type, job_name='dummy')), exist_ok=True)

    number_of_submitted_jobs = 1
    # submit one with all data
    job_long_name = f'all_weighted_true'
    job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name=job_long_name)
    job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type, job_name=job_long_name)
    job_name=f'ns_all'
    cluster_setting=f'sbatch --time=72:00:00 --mem=5G --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
    netstruct_cmd = build_netstructh_cmd('', '', True ,f'all_mac_{mac_min_range}-{mac_max_range}_maf_{maf_min_range}-{maf_max_range}')
    cmd_to_run=f'{cluster_setting} {path_to_wrapper} {netstruct_cmd}'
    if netstruct_cmd:
        print(cmd_to_run)
        subprocess.run(['/cs/icore/amir.rubin2/code/snpnmi/cluster/submit_helper.sh', cmd_to_run])

    # now submit netstruct class by class
    for mac_maf in ['mac', 'maf']:
        netstruct_cmd = None
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        if min_range>=0:
            print(f'go over {mac_maf} values: [{min_range},{max_range}]')
            for val in range(min_range, max_range+1):
                if number_of_submitted_jobs == max_number_of_jobs:
                    break
                # in maf we take 0.x
                if not is_mac:
                    val = f'{val * 1.0/100}'
                job_long_name = f'{mac_maf}{val}_weighted_true'
                job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name=job_long_name)
                job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type, job_name=job_long_name)
                job_name=f'ns_{val}'
                cluster_setting=f'sbatch --time=72:00:00 --mem=5G --error="{job_stderr_file}" --output="{job_stdout_file}" --job-name="{job_name}"'
                netstruct_cmd = build_netstructh_cmd(mac_maf, val)
                if netstruct_cmd:
                    cmd_to_run=f'{cluster_setting} {path_to_wrapper} {netstruct_cmd}'
                    print(cmd_to_run)
                    subprocess.run(['/cs/icore/amir.rubin2/code/snpnmi/cluster/submit_helper.sh', cmd_to_run])
                    number_of_submitted_jobs += 1
                    if number_of_submitted_jobs == max_number_of_jobs:
                        print(f'No more jobs will be submitted.')
                        break


def build_netstructh_cmd(mac_maf, val, run_on_all=False, all_classes_str=None):
    paths_helper = get_paths_helper()
    jar_path = paths_helper.netstruct_jar
    output_folder = paths_helper.netstruct_folder + f'{mac_maf}_{val}_all/'
    if run_on_all:
        output_folder = paths_helper.netstruct_folder + all_classes_str + '/'

    os.makedirs(output_folder, exist_ok=True)

    # per class /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/distances/mac_2_all_norm_dist.tsv.gz
    distances_matrix_path = paths_helper.dist_folder + f'{mac_maf}_{val}_all_norm_dist.tsv.gz'
    if run_on_all:
    # all classes /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/distances/all_mac_2-18_maf_1-49_norm_dist.tsv.gz
        distances_matrix_path = paths_helper.dist_folder + all_classes_str + '_norm_dist.tsv.gz'
    # validate the input
    if not os.path.isfile(distances_matrix_path.replace('.tsv.gz', '.valid.flag')):
        print(f'{distances_matrix_path} doesnt have a valid.flag file to validate it!')
        return None

    indlist_path = paths_helper.netstructh_indlist_path
    sample_sites_path = paths_helper.netstructh_sample_sites_path
    return f'java -jar {jar_path} -ss 0.001 -minb 5 -mino 5 -pro {output_folder} -pm {distances_matrix_path} -pmn {indlist_path} -pss {sample_sites_path} -w true'

#submit_netstruct_per_class(2, 18, 1, 49, 70)

if __name__ == '__main__':
    # by mac
    mac_min_range = int(sys.argv[1])
    mac_max_range = int(sys.argv[2])

    # by maf
    maf_min_range = int(sys.argv[3])
    maf_max_range = int(sys.argv[4])

    # submission details
    # maybe support also these in the future for slices
    #min_window_index =  int(sys.argv[5])
    #max_window_index =  int(sys.argv[6])
    max_number_of_jobs =  int(sys.argv[5])

    # print the inputs
    print('mac_min_range', mac_min_range)
    print('mac_max_range', mac_max_range)
    print('maf_min_range', maf_min_range)
    print('maf_max_range', maf_max_range)
    print('max_number_of_jobs', max_number_of_jobs)

    submit_netstruct_per_class(mac_min_range, mac_max_range, maf_min_range, maf_max_range, max_number_of_jobs)
