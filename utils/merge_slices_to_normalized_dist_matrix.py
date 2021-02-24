# TODO cmd

import sys
import time
import random
import os
import glob
import gzip

from os.path import dirname, abspath
root_path = dirname(dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

from utils.common import normalize_distances, get_number_of_windows_by_class, build_empty_upper_left_matrix, write_upper_left_matrix_to_file, str2bool, get_paths_helper

def add_from_file_to_values(path, num_ind, values, cast_method):
    with gzip.open(path, 'rb') as f:
        line = f.readline().decode()
        i = -1
        while line:
            i += 1
            parts = line.replace('\n','').split()
            assert len(parts) == num_ind - 1 - i
            for j, value in enumerate(parts):
                values[i][j] += cast_method(value)
            line = f.readline().decode()
        # minus 1 as we only have i-j (wihtou i-i) minus another one as the count is zero based 
        assert i == num_ind - 1 - 1

def merge_distances_based_on_files(dist_files, count_files):
    assert len(dist_files) == len(count_files)
    # use the first file to understand the number of individuals
    with gzip.open(dist_files[0], 'rb') as f:
        num_ind = len(f.readline().split()) + 1
    dists = build_empty_upper_left_matrix(num_ind, 0.0)
    counts = build_empty_upper_left_matrix(num_ind, 0)

    # sum up the distances (and counts) file by file.
    for path_i, (dist_path, count_path) in enumerate(zip(dist_files, count_files)):
        if path_i % 10 == 0:
            print(f'{time.time()}: process file {path_i+1}/{len(dist_files)}')
        add_from_file_to_values(dist_path, num_ind, dists, float)
        add_from_file_to_values(count_path, num_ind, counts, int)
    return dists, counts


def write_metadata_to_file(dist_files, count_files, metadata_file_path):
    with open(metadata_file_path,'w') as f:
        files_s = '\n'.join([i for i in dist_files])
        f.write(f'dist files used:\n{files_s}')
        files_s = '\n'.join([i for i in count_files])
        f.write(f'\n\ncount files used:\n{files_s}')

def collect_files(slices_folder, min_mac, max_mac, min_maf, max_maf, details_type):
    matched_files = []
    for mac in range(min_mac, max_mac+1):
        path_regex = slices_folder + f'mac_{mac}/slice_' + '*_' + details_type +'.tsv.gz'
        matched_files.append(glob.glob(path_regex))
    for maf_int in range(min_maf, max_maf+1):
        # TODO - check this for maf 0.4!
        maf = f'{maf_int*1.0/100}'
        path_regex = slices_folder + f'maf_{maf}/slice_' + '*_' + details_type +'.tsv.gz'
        matched_files += glob.glob(path_regex)
    return matched_files

def merge_all_slices_to_normalized_dist_matrix(min_mac, max_mac, min_maf, max_maf):
    # prepare paths
    output_files_suffix=f'_mac_{min_mac}_{max_mac}_maf_{min_maf}_{max_maf}.gz.tsv'
    paths_helper = get_paths_helper()

    merged_path = paths_helper.data_folder + 'merged/'
    os.makedirs(merged_path, exist_ok=True)

    merged_norm_distances_file = merged_path + 'merged_norm_dist' + output_files_suffix
    merged_distances_file = merged_path + 'merged_dist' + output_files_suffix
    merged_counts_file = merged_path + 'merged_count' + output_files_suffix
    merged_metadata_file = merged_path + 'merged_metadata' + output_files_suffix

    # collect files
    slices_folder = paths_helper.slices_folder
    dist_files = collect_files(slices_folder, min_mac, max_mac, min_maf, max_maf, 'dist')
    count_files = collect_files(slices_folder, min_mac, max_mac, min_maf, max_maf, 'count')
    print(f'will process {len(dist_files)} files')
    write_metadata_to_file(dist_files, count_files, merged_metadata_file)

    # merge files
    distances, counts = merge_distances_based_on_files(dist_files, count_files)
    norm_distances = normalize_distances(distances, counts)
    write_upper_left_matrix_to_file(merged_norm_distances_file, norm_distances)
    print(f'merged_norm_distances_file : {merged_norm_distances_file}')
    
    write_upper_left_matrix_to_file(merged_distances_file, distances)
    print(f'merged_distances_file : {merged_distances_file}')
    
    write_upper_left_matrix_to_file(merged_counts_file, counts)
    print(f'merged_counts_file : {merged_counts_file}')

def main(args):
    s = time.time()
    print ('Number of arguments:', len(args), 'arguments.')
    print ('Argument List:', str(args))
    min_mac = int(args[0])
    assert min_mac >= 0
    max_mac = int(args[1])
    assert max_mac >= 0
    min_maf = int(args[2])
    assert min_maf >= 0
    max_maf = int(args[3])
    assert max_maf >= 0

    print('min_mac',min_mac)
    print('max_mac',max_mac)
    print('min_maf',min_maf)
    print('max_maf',max_maf)

    merge_all_slices_to_normalized_dist_matrix(min_mac, max_mac, min_maf, max_maf)

    print(f'{(time.time()-s)/60} minutes total run time')

# params
# min_mac=1
# max_mac=0
# min_maf=49
# max_maf=49
# main([min_mac, max_mac, min_maf, max_maf])

if __name__ == "__main__":
   main(sys.argv[1:])