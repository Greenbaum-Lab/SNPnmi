import gzip
import time
import sys
import os
from os.path import dirname, abspath
import numpy as np

root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)

from utils.loader import Timer
from utils.common import build_empty_upper_left_matrix, write_upper_left_matrix_to_file, write_pairwise_similarity
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
def calc_similarity_based_on_files(similarity_files, count_files):
    similarity_result = None   # We can't tell similarity_result shape yet.
    count_all_counts = None
    for similarity_file, count_file in zip(similarity_files, count_files):
        with open(similarity_file, 'rb') as sim:
            simi_mat = np.load(sim)
        if similarity_result is None:
            similarity_result = np.zeros_like(simi_mat)
            count_all_counts = np.zeros_like(simi_mat)
        with open(count_file, 'rb') as count:
            count_mat = np.load(count)
        add_result = np.divide(simi_mat, count_mat)
        assert 0 <= np.min(add_result) <= np.max(add_result) <= 1
        similarity_result += np.divide(simi_mat, count_mat)
        count_all_counts += count_mat
    return similarity_result, count_mat



def generate_similarity_matrix(similarity_files, count_files, output_folder, output_files_name, override=False):
    # validate output paths - check that we dont override if we should not
    all_count_file = f'{output_files_name}_count.npy'
    all_similarity_file = f'{output_files_name}_similarity.npy'
    if (not override) and os.path.isfile(all_count_file):
        print(f'all_count_distances_file exist, do not calc! {all_count_file}')
        return
    os.makedirs(output_folder, exist_ok=True)
    

    # validate input - break if not valid
    # all_valid, promlematic_file = _validate_count_dist_files(windows_files)
    # if not all_valid:
    #     raise Exception(f'promlematic_file: {promlematic_file}')

    # calc distances and counts
    with Timer("Actual job"):
        similarity, counts = calc_similarity_based_on_files(similarity_files, count_files)

    # write (and validate) output
    write_pairwise_similarity(all_similarity_file, similarity, all_count_file, counts)
    print(f'all_similarity_file : {all_similarity_file}')
    # _validate_count_dist_file(all_count_file)

    # norm_distances = normalize_distances(similarity, counts)
    # write_upper_left_matrix_to_file(all_norm_similarities_file, norm_distances)
    # print(f'all_norm_similarities_file : {all_norm_similarities_file}')
    # _validate_count_dist_file(all_norm_similarities_file)
