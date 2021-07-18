import gzip
import time
import sys
import os
from os.path import dirname, abspath
root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)
from utils.common import  build_empty_upper_left_matrix, write_upper_left_matrix_to_file, write_pairwise_distances
from utils.validate import _validate_count_dist_file, _validate_count_dist_files

# TODO rename - similiarty
def normalize_distances(distances, counts):
    num_ind = len(distances) + 1
    norm_dists = build_empty_upper_left_matrix(num_ind, 0.0)
    for r_i, (r_dist, r_count) in enumerate(zip(distances, counts)):
        for c_i, (c_dist, c_count) in enumerate(zip(r_dist, r_count)):
            norm_dists[r_i][c_i] = float(c_dist)/float(c_count)
    return norm_dists

# TODO rename - similiarty
def calc_distances_based_on_files(files):
    # use the first file to understand the number of individuals
    with gzip.open(files[0], 'rb') as f:
        num_ind = len(f.readline().split()) + 1
    dists = build_empty_upper_left_matrix(num_ind, 0.0)
    counts = build_empty_upper_left_matrix(num_ind, 0)

    # sum up the distances (and counts) file by file.
    file_i = 0
    print(f'{time.time()}: process file 1/{len(files)}')
    for path in files:
        file_i += 1
        if file_i % 10 == 0:
            print(f'{time.time()}: process file {file_i}/{len(files)}')
        with gzip.open(path, 'rb') as f:
            line = f.readline().decode()
            i = -1
            while line:
                i += 1
                parts = line.replace('\n','').split()
                assert len(parts) == num_ind - 1 - i
                for j, count_dist in enumerate(parts):
                    count, dist = count_dist.split(';', 2)
                    counts[i][j] += int(count)
                    dists[i][j] += float(dist)
                line = f.readline().decode()
            # minus 1 as we only have i to j (without i to i) minus another one as the count is zero based
            assert i == num_ind - 1 - 1
    return dists, counts

# TODO - change to similarity
def generate_similarity_matrix_TODO(windows_files, output_folder, output_files_name, override=False):
    # validate output paths - check that we dont override if we should not
    all_count_distances_file = f'{output_folder}{output_files_name}_count_dist.tsv.gz'
    all_norm_distances_file = f'{output_folder}{output_files_name}_norm_dist.tsv.gz'
    if (not override) and os.path.isfile(all_count_distances_file):
        print(f'all_count_distances_file exist, do not calc! {all_count_distances_file}')
        return
    os.makedirs(output_folder, exist_ok=True)
    

    # validate input - break if not valid
    all_valid, promlematic_file = _validate_count_dist_files(windows_files)
    if not all_valid:
        raise Exception(f'promlematic_file: {promlematic_file}')

    # calc distances and counts
    dists, counts = calc_distances_based_on_files(windows_files)

    # write (and validate) output
    write_pairwise_distances(all_count_distances_file, counts, dists)
    print(f'all_count_distances_file : {all_count_distances_file}')
    _validate_count_dist_file(all_count_distances_file)

    norm_distances = normalize_distances(dists, counts)
    write_upper_left_matrix_to_file(all_norm_distances_file, norm_distances)
    print(f'all_norm_distances_file : {all_norm_distances_file}')
    _validate_count_dist_file(all_norm_distances_file)