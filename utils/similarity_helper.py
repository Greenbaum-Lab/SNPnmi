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
            norm_dists[r_i][c_i] = float(c_dist) / float(c_count)
    return norm_dists


def calc_similarity_based_on_files(similarity_files, count_files):
    similarity_result = None  # We can't tell similarity_result shape yet.
    count_all_counts = None
    for similarity_file, count_file in zip(similarity_files, count_files):
        assert similarity_file[-12:] == count_file[-12:], "Using different windows!!"
        with open(similarity_file, 'rb') as sim:
            simi_mat = np.load(sim)
        if similarity_result is None:
            similarity_result = np.zeros_like(simi_mat)
            count_all_counts = np.zeros_like(simi_mat)
        with open(count_file, 'rb') as count:
            count_mat = np.load(count)
        similarity_result += simi_mat
        count_all_counts += count_mat

    similarity_result = np.divide(similarity_result, count_all_counts)
    assert 0 <= np.min(similarity_result) <= np.max(similarity_result) <= 1
    return similarity_result, count_all_counts


def generate_similarity_matrix(similarity_files, count_files, output_folder, output_files_name, override=False):
    # validate output paths - check that we don't override if we should not
    all_count_file = f'{output_files_name}_count.npy'
    all_similarity_file = f'{output_files_name}_similarity.npy'
    if (not override) and os.path.isfile(all_count_file) and os.path.isfile(all_similarity_file):
        print(f'count and similarity files exist, do not calc! {all_count_file}')
        return
    os.makedirs(output_folder, exist_ok=True)

    # calc distances and counts
    similarity, counts = calc_similarity_based_on_files(similarity_files, count_files)

    # write (and validate) output
    write_pairwise_similarity(all_similarity_file, similarity, all_count_file, counts)


def file012_to_numpy(input_file_path, raw_file=None):
    if raw_file is None:
        with open(input_file_path, 'rb') as f:
            raw_file = f.read().decode()
    split_individuals = raw_file.split('\n')
    if split_individuals[-1] == '':  # we throw empty line at the end of the file
        split_individuals = split_individuals[:-1]
    split_sites = [individual.split('\t') for individual in split_individuals]
    arr = np.array(split_sites, dtype=np.int8)
    if np.any(arr[:, 0] > 2):
        arr = arr[:, 1:]  # First column is individual number.
    return arr


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


def matrix_to_edges_file(input_numpy_path, edges_file_path):
    with open(input_numpy_path, 'rb') as f:
        matrix = np.load(f)
    max_e = np.max(matrix)
    num_of_indv = matrix.shape[0]
    result_file = ""
    for i in range(num_of_indv):
        for j in range(i + 1, num_of_indv, 1):
            result_file += f"{i} {j} {matrix[i, j] / max_e}\n"
    with open(edges_file_path, 'w') as f:
        f.write(result_file[:-1])
