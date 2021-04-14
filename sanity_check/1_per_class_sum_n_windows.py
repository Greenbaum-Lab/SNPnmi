# given a class and N, we will take N windows from the class and create a distance matrix based on them
# python3 1_per_class_sum_n_windows.py mac 18 0 100

# takes ~40 seconds for 100 windows.
# 6 minutes for 500 windows.
# 10k windows will take 2 hours
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

def sum_windows(mac_maf, class_name, min_window_index, max_window_index, count_dist_window_template, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    max_window_index_in_class = get_number_of_windows_by_class()[str(class_name)] - 1 # windows are zero based
    # if we have less than max_window_index, we will use the max available
    max_window_index = min(max_window_index, max_window_index_in_class)

    slice_counts_dist_file = f'{output_dir}{mac_maf}_{class_name}_{min_window_index}-{max_window_index}_count_dist.tsv.gz'
    if os.path.isfile(slice_counts_dist_file):
        print(f'slice_distances_file exist, do not calc! {slice_counts_dist_file}')
        return
    slice_norm_distances_file = f'{output_dir}{mac_maf}_{class_name}_{min_window_index}-{max_window_index}_norm_dist.tsv.gz'
    windows_files = [count_dist_window_template.format(window_index=index) for index in range(min_window_index, max_window_index + 1)]

    dists, counts = calc_distances_based_on_files(windows_files)

    write_pairwise_distances(slice_counts_dist_file, counts, dists)
    print(f'slice_counts_dist_file : {slice_counts_dist_file}')

    norm_distances = normalize_distances(dists, counts)
    write_upper_left_matrix_to_file(slice_norm_distances_file, norm_distances)
    print(f'slice_norm_distances_file : {slice_norm_distances_file}')


def main(args):
    s = time.time()
    print ('Number of arguments:', len(args), 'arguments.')
    print ('Argument List:', str(args))
    mac_maf = args[0]
    assert mac_maf=='mac' or mac_maf=='maf'
    class_name = args[1]
    min_window_index = int(args[2])
    max_window_index = int(args[3])
    assert min_window_index>=0
    assert max_window_index>=0
    assert min_window_index<=max_window_index

    print('mac_maf',mac_maf)
    print('class_name',class_name)
    print('min_window_index',min_window_index)
    print('max_window_index',max_window_index)

    # Prepare paths
    paths_helper = get_paths_helper()

    # /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/windows/mac_2/count_dist_window_724.tsv.gz
    count_dist_window_template = paths_helper.count_dist_window_template.replace('{mac_maf}',mac_maf).replace('{class_name}',class_name)

    output_dir = paths_helper.dist_folder
    print('count_dist_window_template', count_dist_window_template)
    print('output_dir',output_dir)

    sum_windows(
        mac_maf,
        class_name,
        min_window_index,
        max_window_index,
        count_dist_window_template,
        output_dir)
    print(f'{(time.time()-s)/60} minutes total run time')

# mac_maf = 'maf'
# class_name = '0.49'
# min_window_index = 0
# max_window_index = 5
# main([mac_maf, class_name, min_window_index, max_window_index])

if __name__ == "__main__":
   main(sys.argv[1:])
