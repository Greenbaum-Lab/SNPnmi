# specific input 012 file:
# python3 calc_distances_in_window.py maf 1 0 5
import numpy as np
import pandas as pd
import json
import os
import gzip
import sys
import time
import sys
from os.path import dirname, abspath

from utils.loader import Timer

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from steps.s4_calc_similarity.calc_similarity_helper import check_guardrails, site_calc_pairwise_similarity, get_012_df
from utils.common import build_empty_upper_left_matrix, get_paths_helper, write_pairwise_similarity, AlleleClass, \
    args_parser


# removed min_valid_sites_precentage = 0.1


def window_calc_pairwise_similarities_with_guardrails(window_df, min_valid_sites_percentage, min_minor_expected,
                                                      max_minor_expected, mac_maf):
    # for each column, we calc the pairwise distances and add it to the grand total
    # for performance, we use 2 lists of lists, one for distances and one for counts

    num_of_individuals = len(window_df)
    window = window_df.to_numpy().astype(float)
    window[window == -1] = np.nan
    is_valid_window = (~np.isnan(window)).astype(np.uint8)

    num_valid_genotypes = np.sum(is_valid_window, axis=0)
    non_ref_count = np.sum(window == 1, axis=0) + 2 * np.sum(window == 2, axis=0)
    non_ref_freq = non_ref_count / (2 * num_valid_genotypes)
    ref_freq = 1 - non_ref_freq
    check_guardrails(mac_maf, max_minor_expected, min_minor_expected, min_valid_sites_percentage, non_ref_count,
                     non_ref_freq, num_of_individuals, num_valid_genotypes)

    # Trick to avoid computation of nan values one by one
    window0 = window.copy()
    window0[np.isnan(window0)] = 0
    window2 = window.copy()
    window2[np.isnan(window2)] = 2

    # Compute similarity
    first_element = (ref_freq * window0) @ window0.T
    second_element = (non_ref_freq * (2 - window2)) @ (2 - window2).T
    similarity = (first_element + second_element) / 4
    window_pairwise_counts = is_valid_window @ is_valid_window.T

    return matrix2upper_tri_list(window_pairwise_counts), matrix2upper_tri_list(similarity)


def check_guardrails(mac_maf, max_minor_expected, min_minor_expected, min_valid_sites_percentage, non_ref_count,
                     non_ref_freq, num_of_individuals, num_valid_genotypes):
    assert np.min(num_valid_genotypes / num_of_individuals) > min_valid_sites_percentage
    if mac_maf == 'mac':
        assert min_minor_expected <= np.min(non_ref_count) <= np.max(non_ref_count) <= max_minor_expected
    else:
        assert min_minor_expected <= np.min(non_ref_freq) <= np.max(non_ref_freq) <= max_minor_expected


def matrix2upper_tri_list(matrix):
    assert matrix.shape[0] == matrix.shape[1]
    assert matrix.ndim == 2
    length = matrix.shape[0]
    results = []
    for i in range(length - 1):
        results.append(matrix[i, i + 1:])
    return results


def site_calc_pairwise_similarity_with_guardrails(window_df, site_index, min_valid_sites_precentage, min_minor_expected,
                                                  max_minor_expected, window_pairwise_counts,
                                                  window_pairwise_similarity, mac_maf):
    # extract only the genotypes of the specific site from this window
    genotypes = window_df.iloc[:, site_index].values
    # get counts
    num_individuals = len(genotypes)
    # count the amount of not -1 in alleles
    num_valid_genotypes = len(genotypes[genotypes != -1])
    non_ref_count = sum(genotypes[genotypes > 0])
    ref_count = 2 * num_valid_genotypes - non_ref_count
    non_ref_freq = float(non_ref_count) / (2 * num_valid_genotypes)
    ref_freq = float(ref_count) / (2 * num_valid_genotypes)
    # print(f'Site index: {site_index}, non ref allele frequency: {non_ref_freq}')
    # print(f'Site index: {site_index}, ref allele frequency: {ref_freq}')
    # guardrails
    assert abs(ref_freq + non_ref_freq - 1) < 1e-04
    check_guardrails(num_individuals, num_valid_genotypes, ref_count, non_ref_count, min_valid_sites_precentage,
                     min_minor_expected, max_minor_expected, mac_maf)
    site_calc_pairwise_similarity(genotypes, num_individuals, ref_freq, non_ref_freq, window_pairwise_counts,
                                  window_pairwise_similarity)


def calc_similarity_in_windows(dataset_name, mac_maf, class_value, min_window_index, max_window_index,
                               min_valid_sites_precentage=0.1):
    allele_class = AlleleClass(mac_maf, class_value)
    class_name = allele_class.class_name
    min_minor_expected = allele_class.class_min_val
    max_minor_expected = allele_class.class_max_val

    # prepare paths
    path_helper = get_paths_helper(dataset_name)
    similarity_output_dir = path_helper.similarity_by_class_folder_template.format(class_name=class_name)
    os.makedirs(similarity_output_dir, exist_ok=True)

    for window_id in range(min_window_index, max_window_index + 1):

        input_012_file = path_helper.window_by_class_template.format(class_name=class_name, window_id=window_id)
        print(f'process {input_012_file}')
        output_count_similarity_file = path_helper.similarity_by_class_and_window_template.format(class_name=class_name,
                                                                                                  window_id=window_id)
        if os.path.isfile(output_count_similarity_file) and False:
            print(f'output_count_similarity_file exist, do not calc! {output_count_similarity_file}')
            continue
        window_df = get_012_df(input_012_file)
        # used for local debug as it takes a long time to go over 100 sites window_df = window_df[[f'idx{i}' for i in range(1,4)]]

        window_pairwise_counts, window_pairwise_similarity = window_calc_pairwise_similarities_with_guardrails(
            window_df, min_valid_sites_precentage, min_minor_expected, max_minor_expected, mac_maf)
        print(f'output similarity file to {output_count_similarity_file}')

        write_pairwise_similarity(output_count_similarity_file, window_pairwise_counts, window_pairwise_similarity)


def main(options):
    print('Number of arguments:', len(options.args), 'arguments.')
    print('Argument List:', str(options.args))
    dataset_name = options.dataset_name
    mac_maf = options.args[0]
    assert mac_maf == 'mac' or mac_maf == 'maf'

    class_value = int(options.args[1])
    min_window_index = int(options.args[2])
    max_window_index = int(options.args[3])
    assert min_window_index >= 0
    assert max_window_index >= 0
    assert min_window_index <= max_window_index

    print('dataset_name', dataset_name)
    print('mac_maf', mac_maf)
    print('class_value', class_value)
    print('min_window_index', min_window_index)
    print('max_window_index', max_window_index)

    calc_similarity_in_windows(dataset_name, mac_maf, class_value, min_window_index, max_window_index)


# dataset_name = 'hgdp_test'
# mac_maf = 'maf'
# class_value = 1
# min_window_index = 0
# max_window_index = 1
# main([dataset_name, mac_maf, class_value, min_window_index, max_window_index])

if __name__ == '__main__':
    options = args_parser()
    with Timer(f"calc_similarity_in_window on {options.args}"):
        main(options)
