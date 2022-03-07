# python3 submit_netstruct_per_class.py 2 18 1 49 70

import sys
import os
from os.path import dirname, abspath
import time

root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Timer, Loader
from utils.common import get_paths_helper, args_parser, how_many_jobs_run, validate_stderr_empty, class_iter
from utils.similarity_helper import matrix_to_edges_file
from utils.netstrcut_helper import submit_netstruct

job_type = 'netstruct_per_class'


def submit_netstruct_per_class(options):
    paths_helper = get_paths_helper(options.dataset_name)
    stderr_files = []

    for cls in class_iter(options):
        job_long_name = f'{cls.mac_maf}{cls.val}_weighted_true'
        job_name = f'ns_{cls.val}'
        output_folder = paths_helper.net_struct_dir_class.format(class_name=cls.name) + f'{cls.name}_all/'

        similarity_dir = paths_helper.similarity_by_class_folder_template.format(class_name=cls.name)
        similarity_matrix_path = similarity_dir + f'{cls.name}_all_similarity.npy'
        count_matrix_path = similarity_dir + f'{cls.name}_all_count.npy'
        similarity_edges_file = similarity_dir + f'{cls.name}_all_edges.txt'
        matrix_to_edges_file(similarity_matrix_path, count_matrix_path, similarity_edges_file)

        err_file = submit_netstruct(options, job_type, job_long_name, job_name, similarity_edges_file,
                                    output_folder)
        stderr_files.append(err_file)

    with Loader("Running NetStruct_Hierarchy", string_to_find='ns'):
        while how_many_jobs_run(string_to_find="ns"):
            time.sleep(5)

    assert validate_stderr_empty(stderr_files)
    return True


def main(options):
    with Timer(f"netstruct per class with {options.args}"):
        submit_netstruct_per_class(options)
    return True


if __name__ == '__main__':
    arguments = args_parser()
    main(arguments)
