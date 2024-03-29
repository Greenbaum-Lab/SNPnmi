import sys
import os
from os.path import dirname, abspath
import numpy as np

root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)


from utils.common import build_empty_upper_left_matrix, write_pairwise_similarity, handle_hash_file


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
        simi_mat = np.load(similarity_file)['arr_0']
        if similarity_result is None:
            similarity_result = np.zeros_like(simi_mat)
            count_all_counts = np.zeros_like(simi_mat)
        count_mat = np.load(count_file)['arr_0']
        similarity_result += simi_mat
        count_all_counts += count_mat

    return similarity_result, count_all_counts


def generate_similarity_matrix(similarity_files, count_files, output_folder, output_files_name, save_np=False,
                               save_edges=True):
    # validate output paths - check that we don't override if we should not
    to_run = False
    all_count_file = f'{output_files_name}_count.npz'
    all_similarity_file = f'{output_files_name}_similarity.npz'
    edges_file = f'{output_files_name}_edges.txt'
    if save_np:
        if not (os.path.isfile(all_count_file) and os.path.isfile(all_similarity_file)):
            to_run = True
    if save_edges:
        if not os.path.isfile(edges_file):
            to_run = True
    if not to_run:
        return
    os.makedirs(output_folder, exist_ok=True)

    # calc similarities and counts
    similarity, counts = calc_similarity_based_on_files(similarity_files, count_files)

    if save_np:
        write_pairwise_similarity(all_similarity_file, similarity, all_count_file, counts)
    if save_edges:
        matrix_to_edges_file(similarity, counts, edges_file)


def file012_to_numpy(input_file_path):
    final_matrix = np.array([])
    with open(input_file_path, 'r') as f:
        line = f.readline()
        while line:
            if line[-1] == '\n':
                line = line[:-1]
            sites = line.split('\t')
            individual_array = np.array(sites, dtype=np.int16)
            final_matrix = np.vstack((final_matrix, individual_array)) if final_matrix.shape[0] else individual_array
            line = f.readline()
    if np.all(np.arange(final_matrix.shape[0]) == final_matrix[:, 0]):
        final_matrix = final_matrix[:, 1:]  # First column is individual number.
    return final_matrix.astype(np.int8)


def matrix_to_edges_file(similarity_matrix, count_matrix, edges_file_path):
    similarity_matrix = np.true_divide(similarity_matrix, count_matrix)
    max_e = np.max(similarity_matrix)
    num_of_indv = similarity_matrix.shape[0]
    result_file = ""
    for i in range(num_of_indv):
        for j in range(i + 1, num_of_indv, 1):
            result_file += f"{i} {j} {similarity_matrix[i, j] / max_e}\n"
    with open(edges_file_path, 'w') as f:
        f.write(result_file[:-1])


def sum_windows(class_name, windows_id_list, similarity_window_template, count_window_template, output_dir,
                paths_helper):
    similarity_files = [similarity_window_template.format(window_id=index, class_name=class_name) for index in
                        windows_id_list]
    count_files = [count_window_template.format(window_id=index, class_name=class_name) for index in windows_id_list]

    new_hash = handle_hash_file(class_name, paths_helper, [int(wind) for wind in windows_id_list])

    generate_similarity_matrix(similarity_files, count_files, output_dir, f'{output_dir}{class_name}_hash{new_hash}',
                               save_np=False, save_edges=True)

    return new_hash
