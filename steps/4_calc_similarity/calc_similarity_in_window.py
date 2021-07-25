# specific input 012 file:
# python3 calc_distances_in_window.py maf 1 0 5
import pandas as pd
import json
import os
import gzip
import sys
import time
import sys
from os.path import dirname, abspath
from calc_similarity_helper import check_guardrails, site_calc_pairwise_similarity, get_012_df
root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.common import build_empty_upper_left_matrix, get_paths_helper, write_pairwise_similarity, AlleleClass
# TODO move to paths_helper
OUTPUT_PATTERN_DIST_FILE = 'count_dist_window_{window_index}.tsv.gz'

# removed min_valid_sites_precentage = 0.1


def window_calc_pairwise_distances_with_guardrails(window_df, min_valid_sites_precentage, min_minor_freq_expected, max_minor_freq_expected, min_minor_count_expected, max_minor_count_expected):
    # for each column, we calc the pairwise distances and add it to the grand total
    # for performance, we use 2 lists of lists, one for distances and one for counts
    window_pairwise_counts = build_empty_upper_left_matrix(len(window_df), 0)
    window_pairwise_dist = build_empty_upper_left_matrix(len(window_df), 0.0)

    i=0
    for site_index in range(len(window_df.columns)):
        i+=1
        if i%10==0:
            print(f'\tDone {i} out of {len(window_df.columns)} sites in window')
        site_calc_pairwise_similarity_with_guardrails(window_df, site_index, min_valid_sites_precentage, min_minor_freq_expected, max_minor_freq_expected, min_minor_count_expected, max_minor_count_expected, window_pairwise_counts, window_pairwise_dist)
    return window_pairwise_counts, window_pairwise_dist

def site_calc_pairwise_similarity_with_guardrails(window_df, site_index, min_valid_sites_precentage, min_minor_freq_expected, max_minor_freq_expected, min_minor_count_expected, max_minor_count_expected, window_pairwise_counts, window_pairwise_similarity):
    # extract only the genotypes of the specific site from this window
    genotypes = window_df.iloc[:,site_index].values
    # get counts
    num_individuals = len(genotypes)
    # count the amount of not -1 in alleles
    num_valid_genotypes = len(genotypes[genotypes!=-1])
    non_ref_count = sum(genotypes[genotypes>0])
    ref_count = 2*num_valid_genotypes-non_ref_count
    non_ref_freq = float(non_ref_count)/(2*num_valid_genotypes)
    ref_freq = float(ref_count)/(2*num_valid_genotypes)
    #print(f'Site index: {site_index}, non ref allele frequency: {non_ref_freq}')
    #print(f'Site index: {site_index}, ref allele frequency: {ref_freq}')
    # guardrails
    assert abs(ref_freq+non_ref_freq-1)<1e-04
    check_guardrails(num_individuals, num_valid_genotypes, ref_count, non_ref_count, min_valid_sites_precentage, min_minor_freq_expected, max_minor_freq_expected, min_minor_count_expected, max_minor_count_expected)
    site_calc_pairwise_similarity(genotypes, num_individuals, ref_freq, non_ref_freq, window_pairwise_counts, window_pairwise_similarity)

def calc_similarity_in_windows(
    dataset_name,
    mac_maf,
    class_value,
    min_window_index,
    max_window_index,
    min_valid_sites_precentage=0.1):

    allele_class = AlleleClass(mac_maf, class_value)
    class_name = allele_class.class_name
    min_minor_freq_expected = allele_class.class_min_val
    max_minor_freq_expected = allele_class.class_max_val
    # TODO - no need for these two..
    min_minor_count_expected = allele_class.class_min_val
    max_minor_count_expected = allele_class.class_max_val

    # prepare paths
    path_helper = get_paths_helper(dataset_name)
    similarity_output_dir = path_helper.similarity_by_class_folder_template.format(class_name=class_name)
    os.makedirs(similarity_output_dir, exist_ok=True)
    start_time = time.time()
    for window_id in range(min_window_index, max_window_index + 1):

        input_012_file = path_helper.window_by_class_template.format(class_name=class_name, window_id=window_id)
        print(f'process {input_012_file}')
        output_count_similarity_file = path_helper.similarity_by_class_and_window_template.format(class_name=class_name, window_id=window_id)
        window_df = get_012_df(input_012_file)
        # used for local debug as it takes a long time to go over 100 sites window_df = window_df[[f'idx{i}' for i in range(1,4)]]
        if os.path.isfile(output_count_similarity_file):
            print(f'output_count_similarity_file exist, do not calc! {output_count_similarity_file}')
            continue

        window_pairwise_counts, window_pairwise_similarity = window_calc_pairwise_distances_with_guardrails(
            window_df,
            min_valid_sites_precentage,
            min_minor_freq_expected,
            max_minor_freq_expected,
            min_minor_count_expected,
            max_minor_count_expected)

        print(f'output similarity file to {output_count_similarity_file}')
        write_pairwise_similarity(output_count_similarity_file, window_pairwise_counts, window_pairwise_similarity)
        print(f'{(time.time()-start_time)/60} minutes for class: {mac_maf}_{class_name}, window_id: {window_id}')

def main(args):
    s = time.time()
    print ('Number of arguments:', len(args), 'arguments.')
    print ('Argument List:', str(args))
    dataset_name = args[0]
    mac_maf = args[1]
    assert mac_maf=='mac' or mac_maf=='maf'
    class_value = args[2]
    min_window_index = int(args[3])
    max_window_index = int(args[4])
    assert min_window_index>=0
    assert max_window_index>=0
    assert min_window_index<max_window_index

    print('dataset_name',dataset_name)
    print('mac_maf',mac_maf)
    print('class_value',class_value)
    print('min_window_index',min_window_index)
    print('max_window_index',max_window_index)

    calc_similarity_in_windows(
        dataset_name,
        mac_maf,
        class_value,
        min_window_index,
        max_window_index)

    print(f'{(time.time()-s)/60} minutes total run time')

dataset_name = 'hgdp_test'
mac_maf = 'maf'
class_value = 1
min_window_index = 0
max_window_index = 1
main([dataset_name, mac_maf, class_value, min_window_index, max_window_index])

if __name__ == "X__main__":
   main(sys.argv[1:])
