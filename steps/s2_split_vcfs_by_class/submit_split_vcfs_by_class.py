

# Per vcf file, per class, will submit a job (if checkpoint does not exist)
import sys
import time

from os.path import dirname, abspath
root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Loader, Timer
from utils.common import warp_how_many_jobs, args_parser, class_iter
from utils.config import *
from utils.cluster.cluster_helper import submit_to_cluster, submit_to_heavy_lab
from utils.checkpoint_helper import *
from steps.s2_split_vcfs_by_class.split_vcf_by_class import compute_max_min_val

SCRIPT_NAME = os.path.basename(__file__)
job_type = 'split_vcf_by_class'
path_to_python_script_to_run = '{base_dir}snpnmi/steps/s2_split_vcfs_by_class/split_vcf_by_class.py'


def generate_job_long_name(mac_maf, class_val, vcf_file_short_name):
    return f'class_{mac_maf}{class_val}_vcf_{vcf_file_short_name}'


def submit_split_vcfs_by_class(options):
    dataset_name = options.dataset_name
    # prepare output folders
    paths_helper = get_paths_helper(dataset_name)
    output_dir = paths_helper.classes_dir
    vcfs_dir = paths_helper.data_dir
    vcf_files = get_dataset_vcf_files_names(dataset_name)
    vcf_files_short_names = get_dataset_vcf_files_short_names(dataset_name)
    validate_dataset_vcf_files_short_names(dataset_name)
    stderr_files = []
    for cls in class_iter(options):
        stderr_files += submit_one_class_split(cls, options, output_dir, vcf_files, vcf_files_short_names, vcfs_dir)

    jobs_func = warp_how_many_jobs("s2")
    with Loader("Splitting jobs are running", jobs_func):
        while jobs_func():
            time.sleep(5)

    return True


def submit_one_class_split(cls, options, output_dir, vcf_files, vcf_files_short_names, vcfs_dir):
    paths_helper = get_paths_helper(options.dataset_name)
    stderr_files = []
    if is_output_exits(None, cls.int_val, cls.mac_maf, output_dir):
        return []
    # go over vcfs
    for (vcf_file, vcf_file_short_name) in zip(vcf_files, vcf_files_short_names):
        if is_output_exits(None, cls.int_val, cls.mac_maf, output_dir + vcf_file_short_name + '/'):
            continue
        print(f'submit for {vcf_file_short_name} ({vcf_file})', flush=True)
        vcf_full_path = vcfs_dir + vcf_file
        job_long_name = generate_job_long_name(cls.mac_maf, cls.int_val, vcf_file_short_name)
        job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                job_name=job_long_name)
        job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                                job_name=job_long_name)
        stderr_files.append(job_stderr_file)
        job_name = f's2{cls.int_val}_{vcf_file_short_name}'
        python_script_params = f'{cls.mac_maf} {cls.int_val} {vcf_full_path} {vcf_file_short_name} {output_dir}'
        if options.local_jobs:
            python_script_to_run = path_to_python_script_to_run.format(base_dir=get_local_code_dir())
            submit_to_heavy_lab(python_script_to_run, python_script_params, job_stdout_file, job_stderr_file)
        else:
            python_script_to_run = path_to_python_script_to_run.format(base_dir=get_cluster_code_folder())
            submit_to_cluster(options, job_type, job_name, python_script_to_run, python_script_params,
                              job_stdout_file, job_stderr_file)

    stderr_files += submit_upload_vcf_to_gdrive(options, paths_helper)
    return stderr_files


def submit_upload_vcf_to_gdrive(options, paths_helper):
    stderrs = []
    sync_job_type = 'sync_to_gdrive'
    python_script = f'{paths_helper.repo}utils/scripts/sync_to_gdrive.py'
    gdrive_path = f'remote:gili_lab/vcf/{options.dataset_name}/'
    vcf_file_names = [f'{paths_helper.data_dir} + {e}' for e in get_dataset_vcf_files_names(options.dataset_name)]
    for vcf_name in vcf_file_names:
        stdout = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=sync_job_type, job_name=vcf_name)
        stderr = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=sync_job_type, job_name=vcf_name)
        submit_to_cluster(options, sync_job_type, job_name='s2', script_path=python_script,
                          script_args=f'--args {vcf_name},{gdrive_path}',
                          job_stdout_file=stdout, job_stderr_file=stderr)
        stderrs += stderr
    return stderrs


def is_output_exits(class_max_val, class_min_val, mac_maf, output_dir):
    class_max_val, class_min_val, is_mac = compute_max_min_val(class_max_val, class_min_val, mac_maf)

    output_path = f'{output_dir}{mac_maf}_{class_min_val}'

    # early break if the output file already exists
    output_file = f'{output_path}.012'
    if os.path.exists(output_file):
        print(f'output file already exist. Break. {output_file}')
        return True
    return False


def main(options):
    with Timer(f"Submitting split vcf by class with {str_for_timer(options)}"):
        is_done, msg = execute_with_checkpoint(submit_split_vcfs_by_class, SCRIPT_NAME, options)
    return is_done


if __name__ == '__main__':
    arguments = args_parser()
    main(arguments)
