
import gzip
import time
import sys
import os
from os.path import dirname, abspath


root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.config import get_num_individuals
from utils.checkpoint_helper import execute_with_checkpoint
from utils.loader import Timer, Loader
from utils.common import get_paths_helper, args_parser, warp_how_many_jobs, validate_stderr_empty, class_iter
from utils.similarity_helper import generate_similarity_matrix, matrix_to_edges_file
from utils.netstrcut_helper import submit_netstruct

SCRIPT_NAME = os.path.basename(__file__)

def sum_all_classes(options):
    paths_helper = get_paths_helper(options.dataset_name)
    # get inputs
    similarity_files = []
    count_files = []
    for cls in class_iter(options):
        similarity_file = paths_helper.similarity_by_class_folder_template.format(class_name=cls.name) + \
                          f"{cls.name}_all_similarity.npz"
        count_file = paths_helper.similarity_by_class_folder_template.format(class_name=cls.name) + \
                     f"{cls.name}_all_count.npz"
        similarity_files.append(similarity_file)
        count_files.append(count_file)

    class_range_str = f'all'
    output_file_name = paths_helper.similarity_dir + class_range_str
    generate_similarity_matrix(similarity_files, count_files, paths_helper.similarity_dir, output_file_name, save_np=False,
                               save_edges=True)
    return output_file_name, class_range_str

def compute_macs_range(options):
    num_of_indv = get_num_individuals(options.dataset_name)
    num_of_genomes = num_of_indv * 2  # diploid assumption
    first_maf = num_of_genomes / 100
    last_mac = first_maf - 1 if first_maf == int(first_maf) else int(first_maf)
    return 2, int(last_mac)


def sum_all_similarity_run_ns_for_all(options):

    options.mac = compute_macs_range(options)
    options.maf = 1, 49

    output_files_name, all_class_range_str = sum_all_classes(options)
    print(output_files_name)

    paths_helper = get_paths_helper(options.dataset_name)
    job_type = 'netstruct'
    job_long_name = f'netstruct_mac_{options.mac[0]}-{options.mac[1]}_maf_{options.maf[0]}-{options.maf[1]}_ss_{options.ns_ss}'
    job_name = f'ns_{options.mac[0]}-{options.mac[1]}_{options.maf[0]}-{options.maf[1]}'
    similarity_edges_file = output_files_name + '_edges.txt'
    output_folder = paths_helper.net_struct_dir + all_class_range_str + '/'
    print(output_folder)
    err_file = submit_netstruct(options, job_type, job_long_name, job_name, similarity_edges_file, output_folder)

    jobs_func = warp_how_many_jobs('ns')
    with Loader("Running NetStruct_Hierarchy", jobs_func):
        while jobs_func():
            time.sleep(5)

    if not err_file:
        return False

    assert validate_stderr_empty([err_file])
    return True


def main(arguments):
    with Timer(f"run net-struct with {arguments}"):
        is_success, msg = execute_with_checkpoint(sum_all_similarity_run_ns_for_all, SCRIPT_NAME, arguments)
    return is_success
