
DEBUG = False
# given class and window id(s), merge the files from all chrs, generating a single file per class-window.

import sys
from os.path import dirname, abspath

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Timer
from utils.common import args_parser, Cls, get_window_size, load_and_decomp_012_mat, comp_and_save_012_mat
from utils.config import *
from utils.checkpoint_helper import *
import pandas as pd
import numpy as np

SCRIPT_NAME = os.path.basename(__file__)


def merge_class_window_across_chrs(dataset_name, mac_maf, class_int_value, window_id):
    cls = Cls(mac_maf, class_int_value)
    path_helper = get_paths_helper(dataset_name)

    output_class_window_file_path = path_helper.window_by_class_template.format(class_name=cls.name,
                                                                                window_id=window_id)

    # Generate the output folder
    os.makedirs(dirname(output_class_window_file_path), exist_ok=True)

    # Go over chrs, merge the windows from them to our file
    window_full_matrix = np.array([])
    for chr_name in get_dataset_vcf_files_short_names(dataset_name):
        chr_window_path = path_helper.window_by_class_and_chr_np_template.format(class_name=cls.name,
                                                                              chr_name=chr_name, window_id=window_id)
        if not os.path.isfile(chr_window_path):
            # it is theoretically possible that a given window wont have sites in a pecific chr.
            # In such case, the 012 window file of this chr wont exist.
            continue
        chr_window_matrix = load_and_decomp_012_mat(chr_window_path, get_window_size(path_helper))

        if window_full_matrix.size == 0:
            window_full_matrix = chr_window_matrix
        else:
            # assert num of rows is the same
            assert (len(window_full_matrix) == len(chr_window_matrix))
            window_full_matrix = np.concatenate([window_full_matrix, chr_window_matrix], axis=1)

    # assert no missing data
    assert np.nan not in window_full_matrix
    # Assert num of sites in window
    window_size = get_window_size(path_helper)
    assert (abs(window_size - window_full_matrix.shape[1]) <= 1), f'num of columns ({window_full_matrix.shape[1]}) should be' \
                                                             f' {window_size}+-1, window_id={window_id}'

    # output to file
    comp_and_save_012_mat(window_full_matrix, output_class_window_file_path)



def merge_class_windows_across_chrs(options):
    dataset_name = options.dataset_name
    mac_maf, class_value, min_window_id, max_window_id = options.args
    for window_id in range(int(min_window_id), int(max_window_id) + 1):
        if window_id % 100 == 0:
            print(
                f'class: {mac_maf}_{class_value}, window_id: {window_id} (from range: [{min_window_id}, {max_window_id}])')
        merge_class_window_across_chrs(dataset_name, mac_maf, int(class_value), window_id)

    return True


def main(options):
    with Timer(f"merge_all_chrs_to_calss_windows on {str_for_timer(options)}"):
        is_executed, msg = execute_with_checkpoint(merge_class_windows_across_chrs, SCRIPT_NAME, options)
    return is_executed


if __name__ == '__main__':
    arguments = args_parser()
    main(arguments)
