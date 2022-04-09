#  In some cases (when running simulation, or might be in general) the ref allele is not the major allele
#  This arises problems when we generate the 012 matrix. This script changes the vcf such that the major allele
#  is always the ref.

import subprocess
import sys
import os
import time
from os.path import dirname, abspath

from tqdm import tqdm

root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)


from utils.cluster.cluster_helper import submit_to_cluster
from utils.config import get_dataset_vcf_files_names
from utils.checkpoint_helper import execute_with_checkpoint
from utils.loader import Timer, Loader

from utils.common import get_paths_helper, str_for_timer, args_parser, warp_how_many_jobs, validate_stderr_empty

SCRIPT_NAME = os.path.basename(__file__)


def fix_all_chrs(options):
    dataset_name = options.dataset_name
    paths_helper = get_paths_helper(dataset_name)
    chrs_name = get_dataset_vcf_files_names(dataset_name)
    if options.args:
        chrs_name = options.args
    if len(chrs_name) == 1:
        fix_ref_in_vcf_to_be_minor_allele(dataset_name, chrs_name[0])
        return True
    err_files = []
    for chr in chrs_name:
        job_type = 'fix vcf'
        os.makedirs(dirname(paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name='dummy')),
                    exist_ok=True)
        job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name=f'fix_{chr}')
        job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type, job_name=f'fix_{chr}')
        err_files.append(job_stderr_file)
        submit_to_cluster(options, job_type, job_name=f'fix_{chr[-5]}', script_path=os.path.abspath(__file__),
                          script_args=f"-s {options.step} -d {dataset_name} --args {chr}", job_stdout_file=job_stdout_file,
                          job_stderr_file=job_stderr_file, num_hours_to_run=24, memory=8)

    jobs_func = warp_how_many_jobs('fix')
    with Loader("Fixing vcf files", jobs_func):
        while jobs_func():
            time.sleep(5)
    return validate_stderr_empty(err_files)


def is_vcf_already_good(stats_file_path):
    flag = True
    with open(stats_file_path, 'r') as stats_file:
        stats_lines = stats_file.readlines()
        stats_lst = stats_lines[0].split('\t')
        STATS_POS = [i for i in range(len(stats_lst)) if stats_lst[i] == "POS"][0]
        for stats_line in tqdm(stats_lines[1:]):
            stats_line_lst = stats_line.split('\t')
            ref = stats_line_lst[-2].split(":")
            non_ref = stats_line_lst[-1].split(":")
            if float(non_ref[-1]) > float(ref[-1]):
                flag = False
                break
    return flag, STATS_POS, len(stats_lines) - 1

def fix_ref_in_vcf_to_be_minor_allele(dataset_name, vcf_file_name):
    paths_helper = get_paths_helper(dataset_name)
    subprocess.run(['mv', paths_helper.data_dir + vcf_file_name, paths_helper.data_dir + f'old_{vcf_file_name}'])
    vcf_file_path = paths_helper.data_dir + f'old_{vcf_file_name}'
    new_vcf_file_path = paths_helper.data_dir + vcf_file_name
    stats_file_path = paths_helper.vcf_stats_folder + vcf_file_name + '.freq.frq'
    is_good, STATS_POS, num_of_sites = is_vcf_already_good(stats_file_path)
    pbar = tqdm(total=num_of_sites)
    with open(vcf_file_path, "r") as vcf_file, open(stats_file_path, 'r') as stats_file, open(new_vcf_file_path,
                                                                                              'w') as new_f:
        old_vcf_line = vcf_file.readline()
        while old_vcf_line.startswith('#'):
            if "REF" not in old_vcf_line:
                new_f.write(old_vcf_line)
                old_vcf_line = vcf_file.readline()
                continue
            else:
                stats_line_lst = old_vcf_line.split('\t')
                VCF_POS = [i for i in range(len(stats_line_lst)) if stats_line_lst[i] == "POS"][0]
                VCF_REF = [i for i in range(len(stats_line_lst)) if stats_line_lst[i] == "REF"][0]
                new_f.write(old_vcf_line)
                old_vcf_line = vcf_file.readline()

        stats_file.readline()
        stats_line = stats_file.readline()
        while stats_line:
            pbar.update(1)
            stats_line_lst = stats_line.split('\t')
            stats_pos = int(stats_line_lst[STATS_POS])
            old_vcf_line_lst = old_vcf_line.split('\t')
            vcf_pos = int(old_vcf_line_lst[VCF_POS])

            assert vcf_pos == stats_pos
            ref = stats_line_lst[-2].split(":")
            non_ref = stats_line_lst[-1].split(":")
            if float(non_ref[-1]) > float(ref[-1]):
                old_vcf_line_lst[VCF_REF] = str(non_ref[0])
                old_vcf_line = '\t'.join(old_vcf_line_lst)
            new_f.write(old_vcf_line)
            stats_line = stats_file.readline()
            old_vcf_line = vcf_file.readline()
        assert not old_vcf_line
    pbar.close()
    print("Done!")


def main(options):
    with Timer(f"Fix vcf file with {str_for_timer(options)}"):
        is_executed, msg = execute_with_checkpoint(fix_all_chrs, SCRIPT_NAME, options)
    return is_executed


if __name__ == '__main__':
    run_arguments = args_parser()
    main(run_arguments)
