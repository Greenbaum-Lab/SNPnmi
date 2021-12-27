import gzip
import time
import sys
import os
from os.path import dirname, abspath
import numpy as np
import re

root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)

from utils.loader import Timer
from utils.common import build_empty_upper_left_matrix, write_upper_left_matrix_to_file, write_pairwise_similarity



# TODO rename - similiarty
def normalize_distances(distances, counts):
    num_ind = len(distances) + 1
    norm_dists = build_empty_upper_left_matrix(num_ind, 0.0)
    for r_i, (r_dist, r_count) in enumerate(zip(distances, counts)):
        for c_i, (c_dist, c_count) in enumerate(zip(r_dist, r_count)):
            norm_dists[r_i][c_i] = float(c_dist) / float(c_count)
    return norm_dists


def check_similarity_count_correlate(count, similarity):
    return count.replace('count', 'similarity') == similarity


def calc_similarity_based_on_files(similarity_files, count_files):
    similarity_result = None  # We can't tell similarity_result shape yet.
    count_all_counts = None
    for i, (similarity_file, count_file) in enumerate(zip(similarity_files, count_files)):
        assert check_similarity_count_correlate(count_file, similarity_file), f"Using different windows!! file names:\n{similarity_file}\n{count_file}"
        with open(similarity_file, 'rb') as sim:
            simi_mat = np.load(sim)
        if similarity_result is None:
            similarity_result = np.zeros_like(simi_mat)
            count_all_counts = np.zeros_like(simi_mat)
        with open(count_file, 'rb') as count:
            count_mat = np.load(count)
        similarity_result += simi_mat
        count_all_counts += count_mat

    return similarity_result, count_all_counts


def generate_similarity_matrix(similarity_files, count_files, output_folder, output_files_name, override=False):
    # validate output paths - check that we don't override if we should not
    all_count_file = f'{output_files_name}_count.npy'
    all_similarity_file = f'{output_files_name}_similarity.npy'
    if (not override) and os.path.isfile(all_count_file) and os.path.isfile(all_similarity_file):
        print(f'count and similarity files exist, do not calc! {all_count_file}')
        return
    os.makedirs(output_folder, exist_ok=True)

    # calc similarities and counts
    similarity, counts = calc_similarity_based_on_files(similarity_files, count_files)

    # write (and validate) output
    write_pairwise_similarity(all_similarity_file, similarity, all_count_file, counts)
    print("Done generate similarity matrix")


def file012_to_numpy(input_file_path):
    final_matrix = np.array([])
    with open(input_file_path, 'r') as f:
        line = f.readline()
        while line:
            sites = line[:-1].split('\t')
            individual_array = np.array([int(i) for i in sites], dtype=np.int8)
            final_matrix = np.vstack((final_matrix, individual_array)) if final_matrix else individual_array
            line = f.readline()
    if np.any(final_matrix[:, 0] > 2):
        final_matrix = final_matrix[:, 1:]  # First column is individual number.
    return final_matrix


def numpy_to_file012(input_numpy_path, matrix=None):
    if matrix is None:
        with open(input_numpy_path, 'rb') as f:
            matrix = np.load(f)
    result = []
    for line in matrix:
        result.append('\t'.join([str(i) for i in line]))
    result = '\n'.join(result)
    result += '\n'
    return result


def matrix_to_edges_file(similarity_matrix_path, count_matrix_path, edges_file_path):
    with open(similarity_matrix_path, 'rb') as f:
        similarity_matrix = np.load(f)
    with open(count_matrix_path, 'rb') as f:
        count_matrix = np.load(f)
    similarity_matrix = np.true_divide(similarity_matrix, count_matrix)
    max_e = np.max(similarity_matrix)
    num_of_indv = similarity_matrix.shape[0]
    result_file = ""
    for i in range(num_of_indv):
        for j in range(i + 1, num_of_indv, 1):
            result_file += f"{i} {j} {similarity_matrix[i, j] / max_e}\n"
    with open(edges_file_path, 'w') as f:
        f.write(result_file[:-1])
