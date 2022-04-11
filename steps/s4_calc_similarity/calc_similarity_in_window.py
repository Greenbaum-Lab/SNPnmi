import numpy as np
import os
import sys
from os.path import dirname, abspath

from tqdm import tqdm

from utils.config import get_num_individuals

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Timer
from steps.s4_calc_similarity.calc_similarity_helper import check_guardrails, get_012_df
from utils.common import get_paths_helper, write_pairwise_similarity, args_parser, Cls, get_window_size, \
    load_and_decomp_012_mat


def window_calc_pairwise_similarities(window, min_valid_sites_percentage, min_minor_expected, max_minor_expected,
                                      mac_maf):
    """
    The computation of similarity between individual is done by summing for every site the value:
    ((1-freq)(val_1 * val_2) + freq * (2 - val_1)(2 - val2))
    We compute it in a matrix form.
    :param window: numpy array of the input window
    :param min_valid_sites_percentage: of ANY site has lower coverage than this value, we assert (need to clean this
           site in an early stage).
    :param min_minor_expected: min value for freq of minor allele
    :param max_minor_expected: max value for freq of minor allele
    :param mac_maf: 'mac' or 'maf' used to choose compare min/max_minor_expected to ref_count or ref_freq
    :return:list of lists (upper triangular matrix) of the number of valid sites per 2 individuals
            list of lists (upper triangular matrix) of the similarity matrix
    """

    num_of_individuals = window.shape[0]
    window[window == -1] = np.nan
    is_valid_window = (~np.isnan(window)).astype(np.uint8)

    window_pairwise_counts = is_valid_window @ is_valid_window.T
    num_valid_genotypes = np.sum(is_valid_window, axis=0)
    non_ref_count = np.sum(window == 1, axis=0) + 2 * np.sum(window == 2, axis=0)
    ref_count = np.sum(window == 1, axis=0) + 2 * np.sum(window == 0, axis=0)
    non_ref_freq = non_ref_count / (2 * num_valid_genotypes)
    ref_freq = 1 - non_ref_freq
    check_guardrails(mac_maf, max_minor_expected, min_minor_expected, min_valid_sites_percentage, non_ref_count,
                     ref_count, num_of_individuals, num_valid_genotypes)

    # Trick to avoid computation of nan values one by one
    window0 = window.copy()
    window0[np.isnan(window0)] = 0
    window2 = window.copy()
    window2[np.isnan(window2)] = 2

    # Compute similarity
    first_element = (ref_freq * window0) @ window0.T
    second_element = (non_ref_freq * (2 - window2)) @ (2 - window2).T
    similarity = (first_element + second_element) / 4
    np.fill_diagonal(similarity, 0)
    return window_pairwise_counts, similarity


def matrix2upper_tri_list(matrix):
    assert matrix.shape[0] == matrix.shape[1]
    assert matrix.ndim == 2
    length = matrix.shape[0]
    results = []
    for i in range(length - 1):
        results.append(matrix[i, i + 1:])
    return results


def calc_similarity_in_windows(dataset_name, mac_maf, class_value, min_window_index, max_window_index):
    cls = Cls(mac_maf, class_value)

    # prepare paths
    path_helper = get_paths_helper(dataset_name)
    similarity_output_dir = path_helper.similarity_by_class_folder_template.format(class_name=cls.name)
    os.makedirs(similarity_output_dir, exist_ok=True)
    num_of_individuals = get_num_individuals(dataset_name)
    for window_id in tqdm(range(min_window_index, max_window_index)):

        input_012_file = path_helper.window_by_class_template.format(class_name=cls.name, window_id=window_id)
        window_matrix = load_and_decomp_012_mat(input_012_file, num_of_individuals).astype(float)
        compute_similarity_and_save_outputs(path_helper, window_matrix, cls, window_id)


def compute_similarity_and_save_outputs(path_helper, window_matrix, cls, window_id, min_valid_sites_percentage=0.1):

    output_similarity_file = path_helper.similarity_by_class_and_window_template.format(class_name=cls.name,
                                                                                        window_id=window_id)
    output_count_file = path_helper.count_by_class_and_window_template.format(class_name=cls.name,
                                                                              window_id=window_id)
    if os.path.isfile(output_similarity_file) and os.path.isfile(output_count_file):
        print(f'output_count_similarity_file exist, do not calc! {output_similarity_file}')
        return

    window_counts, window_similarity = window_calc_pairwise_similarities(window_matrix, min_valid_sites_percentage,
                                                                         cls.val, cls.max_val, cls.mac_maf)

    write_pairwise_similarity(output_similarity_file, window_similarity, output_count_file,
                              window_counts)


def main(options):
    print('Number of arguments:', len(options.args), 'arguments.')
    print('Argument List:', str(options.args))
    dataset_name = options.dataset_name
    mac_maf = options.args[0]
    assert mac_maf == 'mac' or mac_maf == 'maf'

    class_value = int(options.args[1])
    min_window_index = int(options.args[2])
    max_window_index = int(options.args[3])
    assert 0 <= min_window_index <= max_window_index

    print('dataset_name', dataset_name)
    print('mac_maf', mac_maf)
    print('class_value', class_value)
    print('min_window_index', min_window_index)
    print('max_window_index', max_window_index)

    calc_similarity_in_windows(dataset_name, mac_maf, class_value, min_window_index, max_window_index)


if __name__ == '__main__':
    options = args_parser()
    with Timer(f"calc_similarity_in_window on {options.args}"):
        main(options)
