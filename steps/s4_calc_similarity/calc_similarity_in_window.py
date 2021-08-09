# specific input 012 file:
# python3 calc_distances_in_window.py maf 1 0 5
import numpy as np
import os
import sys
from os.path import dirname, abspath

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Timer
from steps.s4_calc_similarity.calc_similarity_helper import check_guardrails, get_012_df
from utils.common import get_paths_helper, write_pairwise_similarity, AlleleClass, args_parser


def window_calc_pairwise_similarities(window_df, min_valid_sites_percentage, min_minor_expected, max_minor_expected,
                                      mac_maf):
    """
    The computation of similarity between individual is done by summing for every site the value:
    ((1-freq)(val_1 * val_2) + freq * (2 - val_1)(2 - val2))
    We compute it in a matrix form.
    :param window_df: data frame of the input window
    :param min_valid_sites_percentage: of ANY site has lower coverage than this value, we assert (need to clean this
           site in an early stage).
    :param min_minor_expected: min value for freq of minor allele
    :param max_minor_expected: max value for freq of minor allele
    :param mac_maf: 'mac' or 'maf' used to choose compare min/max_minor_expected to ref_count or ref_freq
    :return:list of lists (upper triangular matrix) of the number of valid sites per 2 individuals
            list of lists (upper triangular matrix) of the similarity matrix
    """

    num_of_individuals = len(window_df)
    window = window_df.to_numpy().astype(float)
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


def calc_similarity_in_windows(dataset_name, mac_maf, class_value, min_window_index, max_window_index,
                               min_valid_sites_percentage=0.1):
    allele_class = AlleleClass(mac_maf, class_value)
    class_name = allele_class.class_name
    min_minor_expected = allele_class.class_min_val
    max_minor_expected = allele_class.class_max_val

    # prepare paths
    path_helper = get_paths_helper(dataset_name)
    similarity_output_dir = path_helper.similarity_by_class_folder_template.format(class_name=class_name)
    os.makedirs(similarity_output_dir, exist_ok=True)

    for window_id in range(min_window_index, max_window_index):

        input_012_file = path_helper.window_by_class_template.format(class_name=class_name, window_id=window_id)
        output_similarity_file = path_helper.similarity_by_class_and_window_template.format(class_name=class_name,
                                                                                                  window_id=window_id)
        output_count_file = path_helper.count_by_class_and_window_template.format(class_name=class_name,
                                                                                                  window_id=window_id)
        # if os.path.isfile(output_count_similarity_file):
        #     print(f'output_count_similarity_file exist, do not calc! {output_count_similarity_file}')
        #     continue
        window_df = get_012_df(input_012_file)

        window_pairwise_counts, window_pairwise_similarity = window_calc_pairwise_similarities(
            window_df, min_valid_sites_percentage, min_minor_expected, max_minor_expected, mac_maf)
        print(f'output similarity file to {output_similarity_file}\noutput count file to {output_count_file}')

        write_pairwise_similarity(output_similarity_file, window_pairwise_similarity, output_count_file,
                                  window_pairwise_counts)


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
