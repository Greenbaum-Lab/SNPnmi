
DEBUG = False
# given class and window id(s), merge the files from all chrs, generating a single file per class-window.

import sys
from os.path import dirname, abspath

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Timer
from utils.common import get_paths_helper, AlleleClass, args_parser
from utils.config import *
from utils.checkpoint_helper import *
import pandas as pd
import numpy as np

SCRIPT_NAME = os.path.basename(__file__)


def merge_class_window_across_chrs(dataset_name, mac_maf, class_value, window_id):
    allele_class = AlleleClass(mac_maf, class_value)
    path_helper = get_paths_helper(dataset_name)

    output_class_window_file_path = path_helper.window_by_class_template.format(class_name=allele_class.class_name,
                                                                                window_id=window_id)

    # Generate the output folder
    os.makedirs(dirname(output_class_window_file_path), exist_ok=True)

    # Go over chrs, merge the windows from them to our file
    window_df = pd.DataFrame()
    for chr_name in get_dataset_vcf_files_short_names(dataset_name):
        chr_window_path = path_helper.window_by_class_and_chr_np_template.format(class_name=allele_class.class_name,
                                                                              chr_name=chr_name, window_id=window_id)
        if not os.path.isfile(chr_window_path):
            # it is theorticlly possible that a given window wont have sites in all chrs. In such case, the 012 window file of this chr wont exist.
            continue
        with open(chr_window_path, 'rb') as f:
            chr_window_matrix = np.load(f)
        chr_window_df = pd.DataFrame(data=chr_window_matrix, index=np.arange(chr_window_matrix.shape[0]),
                                     columns=[str(c) + chr_name for c in np.arange(chr_window_matrix.shape[1])])

        if window_df.empty:
            window_df = chr_window_df
        else:
            # assert num of rows is the same
            assert (len(window_df) == len(chr_window_df))

            window_df = window_df.join(chr_window_df)

    # assert no missing data
    assert window_df.isnull().values.any() == False
    # Assert num of sites in window
    # TODO : Write window size in prepare_for_split_to_windows and use it in this assert
    assert (abs(100 - len(window_df.columns)) <= 1), f'num of columns ({len(window_df.columns)}) should be 100+-1, window_id={window_id}'

    # output to file
    window_df.to_csv(output_class_window_file_path, sep='\t', header=False, index=False, compression='gzip')


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
