# will use all classes distances matrixes to create a big matrix with all data
# python3 2_sum_distances_from_all_classes mac 18 0 100

# takes ~40 seconds for 100 windows.
import pandas as pd
import json
import os
import gzip
import sys
import time
import sys
from os.path import dirname, abspath
root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)

from utils.common import get_number_of_windows_by_class, build_empty_upper_left_matrix, write_upper_left_matrix_to_file, get_paths_helper, calc_distances_based_on_files, normalize_distances, write_pairwise_distances

#TODO below not stable


def sum_all_classes(mac_min_range, mac_max_range, maf_min_range, maf_max_range, min_window_index, max_window_index):

    paths_helper = get_paths_helper()

    # get inputs
    windows_files = []
    for mac_maf in ['mac', 'maf']:
            is_mac = mac_maf == 'mac'
            min_range = mac_min_range if is_mac else maf_min_range
            max_range = mac_max_range if is_mac else maf_max_range
            if min_range>0:
                print(f'go over {mac_maf} values: [{min_range},{max_range}]')
                for val in range(min_range, max_range+1):
                    # in maf we take 0.x
                    if not is_mac:
                        val = f'{val * 1.0/100}'
                    # input template
                    # /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/sanity_check/distances/maf_0.04_0-499_count_dist.tsv.gz
                    slice_count_distances_file = f'{paths_helper.sanity_check_dist_folder}{mac_maf}_{val}_{min_window_index}-{max_window_index}_count_dist.tsv.gz'
                    windows_files.append(slice_count_distances_file)

    # calc distances and counts
    dists, counts = calc_distances_based_on_files(windows_files)

    # output results
    output_dir = paths_helper.sanity_check_netstruct_folder
    all_count_distances_file = f'{output_dir}mac_{mac_min_range}-{mac_max_range}_maf_{maf_min_range}-{maf_max_range}_windows_{min_window_index}-{max_window_index}_count_dist.tsv.gz'
    all_norm_distances_file = f'{output_dir}mac_{mac_min_range}-{mac_max_range}_maf_{maf_min_range}-{maf_max_range}_windows_{min_window_index}-{max_window_index}_norm_dist.tsv.gz'
    

    write_pairwise_distances(all_count_distances_file, counts, dists)
    print(f'all_count_distances_file : {all_count_distances_file}')

    norm_distances = normalize_distances(dists, counts)
    write_upper_left_matrix_to_file(all_norm_distances_file, norm_distances)
    print(f'all_norm_distances_file : {all_norm_distances_file}')

def main(args):
    s = time.time()
    print ('Number of arguments:', len(args), 'arguments.')
    print ('Argument List:', str(args))
    # by mac
    mac_min_range = int(args[0])
    mac_max_range = int(args[1])

    # by maf
    maf_min_range = int(args[2])
    maf_max_range = int(args[3])

    # submission details
    min_window_index =  int(args[4])
    max_window_index =  int(args[5])

    # print the inputs
    print('mac_min_range', mac_min_range)
    print('mac_max_range', mac_max_range)
    print('maf_min_range', maf_min_range)
    print('maf_max_range', maf_max_range)
    print('min_window_index', min_window_index)
    print('max_window_index', max_window_index)

    sum_all_classes(mac_min_range, mac_max_range, maf_min_range, maf_max_range, min_window_index, max_window_index)

    print(f'{(time.time()-s)/60} minutes total run time')

# main([2, 18, 1, 49, 0, 499])

if __name__ == "__main__":
   main(sys.argv[1:])