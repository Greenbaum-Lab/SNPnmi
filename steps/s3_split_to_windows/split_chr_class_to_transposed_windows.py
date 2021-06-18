DEBUG = True
# given chr name and class split the sites of the class in the chr to *transposed* files correspanding to windows.
# in the next step we will merge per class and windor the files from all chrs, generating a single file per window, which we then transpose.
# the reason the files are transposed are that the input has sites as columns, and we will read the columns and write them to the output file one by one, so we must write them as rows.
# Note that we may have an issue with class mac2 chr 1, where we have 583227 sites.
# TODO: We may need to split to work in case we have too much sites per class and chr.
import os
import sys
import json
import pandas as pd
import random
import time
from os.path import dirname, abspath
root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)
from utils.common import get_paths_helper, AlleleClass
from utils.config import *
from utils.checkpoint_helper import *

SCRIPT_NAME = os.path.basename(__file__)


def build_windows_indexes_files(dataset_name, mac_maf, class_value, chr_short_name):
    # Removed - this should be done in the previous step! validate_split_vcf_output_stats_file(split_vcf_output_stats_file, num_ind, min_mac, max_mac, min_maf, max_maf, min_chr, max_chr)
    allele_class = AlleleClass(mac_maf, class_value)
    path_helper = get_paths_helper(dataset_name)
    chr_windows_indexes_file = path_helper.windows_indexes_template.format(class_name = allele_class.class_name, chr_name=chr_short_name)
    site_index_2_window_id = json.load(open(chr_windows_indexes_file,'r'))
    

    chr_2_num_of_sites = get_num_of_sites_per_chr(dataset_name, mac_maf, class_value)
    print(f'class {mac_maf}_{class_value}')
    for chr_name in chr_2_num_of_sites.keys():
        print(f'chr {chr_name} has {chr_2_num_of_sites[chr_name]} sites')

    chr_2_index_2_window_id = split_to_windows(chr_2_num_of_sites, window_size)
    validate_windows(chr_2_index_2_window_id, chr_2_num_of_sites, window_size)
    for chr_short_name, index_2_window_id in chr_2_index_2_window_id.items():
        output_file = path_helper.windows_indexes_template.format(class_name = allele_class.class_name, chr_name=chr_short_name)
        os.makedirs(dirname(output_file), exist_ok=True)
        with open(output_file, "w") as f:
            json.dump(index_2_window_id, f)
    return True


def main(args):
    s = time.time()
    dataset_name = args[0]
    is_executed, msg = execute_with_checkpoint(build_windows_indexes_files, SCRIPT_NAME, dataset_name, args)
    print(f'{msg}. {(time.time()-s)/60} minutes total run time')
    return is_executed

def _test_me():
    dataset_name = 'hgdp_test'
    mac_maf = 'maf'
    class_value = 1
    window_size = 100
    build_windows_indexes_files(dataset_name, mac_maf, class_value, window_size)

if DEBUG:
    _test_me()
elif __name__ == '__main__':
    main(sys.argv[1:])

