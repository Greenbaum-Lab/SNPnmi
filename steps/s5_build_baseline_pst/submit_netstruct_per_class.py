# python3 submit_netstruct_per_class.py 2 18 1 49 70

import sys
import os
from os.path import dirname, abspath
import time

root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Timer, Loader
from utils.common import get_paths_helper, args_parser, how_many_jobs_run, validate_stderr_empty
from utils.similarity_helper import matrix_to_edges_file
from utils.netstrcut_helper import submit_netstruct

job_type = 'netstruct_per_class'


def submit_netstruct_per_class(options):
    mac_min_range, mac_max_range = options.mac
    maf_min_range, maf_max_range = options.maf
    paths_helper = get_paths_helper(options.dataset_name)
    stderr_files = []
    # now submit netstruct class by class
    for mac_maf in ['mac', 'maf']:
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        if min_range >= 0:
            print(f'go over {mac_maf} values: [{min_range},{max_range}]')
            for val in range(min_range, max_range + 1):
                # in maf we take 0.x
                if not is_mac:
                    val = f'{val * 1.0 / 100}'
                class_name = f"{mac_maf}_{val}"
                job_long_name = f'{mac_maf}{val}_weighted_true'
                job_name = f'ns_{val}'
                output_folder = paths_helper.net_struct_dir + f'{class_name}_all/'

                similarity_dir = paths_helper.similarity_by_class_folder_template.format(class_name=class_name)
                similarity_matrix_path = similarity_dir + f'{class_name}_all_similarity.npy'
                similarity_edges_file = similarity_dir + f'{class_name}_all_edges.txt'
                matrix_to_edges_file(similarity_matrix_path, similarity_edges_file)

                err_file = submit_netstruct(options, job_type, job_long_name, job_name, similarity_edges_file,
                                            output_folder)
                stderr_files.append(err_file)

    with Loader("Running NetStruct_Hierarchy", string_to_find='ns'):
        while how_many_jobs_run(string_to_find="ns"):
            time.sleep(1)

    assert validate_stderr_empty(stderr_files)
    return True


def main(options):
    submit_netstruct_per_class(options)


if __name__ == '__main__':
    arguments = args_parser()
    with Timer(f"submit_netstruct_per_class with {arguments.args}"):
        main(arguments)
