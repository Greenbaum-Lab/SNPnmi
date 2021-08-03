import pandas as pd
import json
import os
import gzip
import sys
import time
import sys
from os.path import dirname, abspath

root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)


def get_012_df(input_012_path):
    # get number of columns in chr:
    with gzip.open(input_012_path, 'rb') as f:
        num_columns = len(f.readline().decode().split('\t'))
    # print(f'{len(indexes)} / {num_columns-1} sites will be used from file {class_012_path}')
    names = [f'idx{i}' for i in range(num_columns)]
    # read file to pandas df
    # Legacy: (not true any more) we drop the first column as the csv contains the individual id in the first index
    df = pd.read_csv(input_012_path, sep='\t', names=names, compression='gzip')
    return df


def check_guardrails(num_individuals, num_valid_genotypes, ref_count, non_ref_count, min_valid_sites_precentage,
                     min_minor_freq_expected, max_minor_freq_expected, min_minor_count_expected,
                     max_minor_count_expected):
    # print(f'Check guardrails')
    # guardrail #1 - min_valid_sites_precentage
    percentage_valid_sites = float(num_valid_genotypes) / num_individuals
    # print(f'Precentage of valid sites: {percentage_valid_sites}')
    if percentage_valid_sites < min_valid_sites_precentage:
        # print(f'ERROR: % of valid sites is {percentage_valid_sites}, lower than allowd: {min_valid_sites_precentage}.')
        assert percentage_valid_sites < min_valid_sites_precentage

    # guardrail #2 mac/maf validation
    if (min_minor_freq_expected == -1 or max_minor_freq_expected == -1) and (
            min_minor_count_expected == -1 or max_minor_count_expected == -1):
        # print(f'ERROR: min_minor_freq_expected, max_minor_freq_expected or min_minor_count_expected, max_minor_count_expected must be >-1')
        assert (min_minor_freq_expected == -1 or max_minor_freq_expected == -1) and (
                    min_minor_count_expected == -1 or max_minor_count_expected == -1)

    # if maf: validate min_minor_freq_expected and max_minor_freq_expected
    if min_minor_freq_expected > -1 and max_minor_freq_expected > -1:
        minor_count = min(ref_count, non_ref_count)
        minor_freq = float(minor_count) / (2 * num_valid_genotypes)
        # print(f'Minor allele frequency: {minor_freq}')
        if minor_freq < min_minor_freq_expected:
            # print(f'ERROR: minor frequency is too low - {minor_freq}, allowd: {min_minor_freq_expected}.')
            assert minor_freq < min_minor_freq_expected
        if minor_freq > max_minor_freq_expected:
            # print(f'ERROR: minor frequency is too high - {minor_freq}, allowd: {max_minor_freq_expected}.')
            assert minor_freq > max_minor_freq_expected

    # if mac: validate min_minor_count_expected and max_minor_count_expected
    if min_minor_count_expected > -1 and max_minor_count_expected > -1:
        minor_count = min(ref_count, non_ref_count)
        # print(f'Minor allele count: {minor_count}')
        if minor_count < min_minor_count_expected:
            # print(f'ERROR: minor frequency is too low - {minor_freq}, allowd: {min_minor_freq_expected}.')
            assert minor_count < min_minor_count_expected
        if minor_count > max_minor_count_expected:
            # print(f'ERROR: minor frequency is too high - {minor_freq}, allowd: {max_minor_freq_expected}.')
            assert minor_count > max_minor_count_expected
    # print(f'Passed guardrails')


def calc_similarity(i1_val, i2_val, ref_freq, non_ref_freq):
    # from VCFtools manual:
    # "Genotypes are represented as 0, 1 and 2, where the number represent that number of non-reference alleles"
    # So - v1_val and v2_val are the amount of non-ref alleles.
    # The distance function is:
    # 1/4[(1-f_a)(Iac+Iad) + (1-f_b)(Ibc+Ibd)]
    # Now, for each combination of v1_val and v2_val, we can compute the distance.

    # 0,0 - v1_val=0 and v2_val=0:
    #    1/4[(1-f_ref)(1+1) + (1-f_ref)(1+1)] = 1/4[(1-f_ref)(4)] = (1-f_ref)

    # 0,1 - v1_val=0 and v2_val=1:
    #    1/4[(1-f_ref)(1+0) + (1-f_ref)(1+0)] = 1/4[(1-f_ref)(2)] = 1/2(1-f_ref)

    # 0,2 - v1_val=0 and v2_val=2:
    #    1/4[(1-f_ref)(0+0) + (1-f_ref)(0+0)] = 0

    # 1,1 - v1_val=1 and v2_val=1:
    #    1/4[(1-f_non_ref)(1+0) + (1-f_ref)(1+0)] = 1/4[(1-f_non_ref)+(1-f_ref)]

    # 1,2 - v1_val=1 and v2_val=2:
    #    1/4[(1-f_non_ref)(1+1) + (1-f_ref)(0+0)] = 1/4[(1-f_non_ref)(2)] = 1/2(1-f_non_ref)

    # 2,2 - v1_val=2 and v2_val=2:
    #    1/4[(1-f_non_ref)(1+1) + (1-f_non_ref)(1+1)] = 1/4[(1-f_non_ref)(4)] = (1-f_non_ref)

    # Also, note that it is symetric:

    # 1,0 - v1_val=1 and v2_val=0:
    #    1/4[(1-f_ref)(1+1) + (1-f_non_ref)(0+0)] = 1/4[(1-f_ref)(2)] = 1/2(1-f_ref)

    # 2,1 - v1_val=2 and v2_val=1:
    #    1/4[(1-f_non_ref)(1+0) + (1-f_non_ref)(0+1)] = 1/4[(1-f_non_ref)(2)] = 1/2(1-f_non_ref)

    # 2,0 - v1_val=2 and v2_val=0:
    #    1/4[(1-f_non_ref)(0+0) + (1-f_non_ref)(0+0)] = 0

    # formula to use: 
    #   (1-f_non_ref)(v1_val*v2_val) + (1-f_ref)((2-v1_val)*(2-v2_val))/4
    dist = ((1 - non_ref_freq) * (i1_val * i2_val) + (1 - ref_freq) * ((2 - i1_val) * (2 - i2_val))) / 4
    assert dist >= 0, dist
    assert dist <= 1, dist
    return dist


def calc_similarity_directly(i1_val, i2_val, ref_freq, non_ref_freq):
    # here we take into account the referenced and not referened allele, the compute is similar to that of calc_similarity
    # set a,b
    # both ref
    if i1_val == 0:
        a = 'ref'
        b = 'ref'
    if i1_val == 1:
        a = 'non_ref'
        b = 'ref'
    # both non ref
    if i1_val == 2:
        a = 'non_ref'
        b = 'non_ref'
    # set c,d
    if i2_val == 0:
        c = 'ref'
        d = 'ref'
    if i2_val == 1:
        c = 'non_ref'
        d = 'ref'
    if i2_val == 2:
        c = 'non_ref'
        d = 'non_ref'
    Iac = 1 if a == c else 0
    Iad = 1 if a == d else 0
    Ibc = 1 if b == c else 0
    Ibd = 1 if b == d else 0
    f_a = ref_freq if a == 'ref' else non_ref_freq
    f_b = ref_freq if b == 'ref' else non_ref_freq
    dist = (((1 - f_a) * (Iac + Iad)) + ((1 - f_b) * (Ibc + Ibd))) / 4.0
    assert dist >= 0, dist
    assert dist <= 1, dist
    return dist


def site_calc_pairwise_similarity(genotypes, num_individuals, ref_freq, non_ref_freq, window_pairwise_counts,
                                  window_pairwise_similarity):
    for i1 in range(num_individuals - 1):
        i1_val = genotypes[i1]
        # if this entry is not valid for i1, no need to go over all the others, nothing to add to freq nor counts
        # if i1%100==0:
        #    print(f'Done with individual {i1}/{len(genotypes)}')
        if i1_val == -1:
            continue
        for i2 in range(i1 + 1, num_individuals):
            i2_val = genotypes[i2]
            if i2_val == -1:
                continue
            else:
                # this is a valid entry, we add 1 to the count
                window_pairwise_counts[i1][i2 - i1 - 1] += 1
                window_pairwise_similarity[i1][i2 - i1 - 1] += calc_similarity(i1_val, i2_val, ref_freq, non_ref_freq)


# we no longet need this. step 3.3 generated the files for us.
def get_window_deprecated(class_012_path_template, windows_indexes_path, mac_maf, class_name, window_index):
    # read indexes of window
    with open(windows_indexes_path) as f:
        windows_indexes = json.load(f)
    window_indexes = windows_indexes[window_index]
    print(f'There are {len(window_indexes)} indexes in window indexes list')
    chr_id2indexes = dict()
    print('Read sites from appropriate chrs')
    for chr_id_index in window_indexes:
        chr_id, index = chr_id_index.split(';', 1)
        # if chr_id not in the dict, we get an empty list
        indexes = chr_id2indexes.get(chr_id, [])
        indexes.append(int(index))
        chr_id2indexes[chr_id] = indexes
    # no need to sort as we supply this list to pandas
    # sort the indexes so that we can easily read the file
    # {k:v.sort() for k, v in chr_id2indexes.items()}

    # go over the chr_ids, and get from the file the correct columns
    class_012_df = pd.DataFrame()
    for chr_id, indexes in chr_id2indexes.items():
        class_name_for_012 = class_name
        if mac_maf == 'maf':
            class_name_for_012 = f'{float(class_name):.2f}'
        class_012_path = class_012_path_template.format(chr_id=chr_id, mac_maf=mac_maf, class_name=class_name_for_012)
        # get number of columns in chr:
        with open(class_012_path, 'r') as f:
            num_columns = len(f.readline().split('\t'))
        # print(f'{len(indexes)} / {num_columns-1} sites will be used from file {class_012_path}')
        names = [f'chr{chr_id}_idx{i}' for i in range(num_columns)]
        # we add one as the csv contains the individual id in the first index
        indexes_to_use = [i + 1 for i in indexes]
        # read file to pandas df
        selected_indexes_012_df = pd.read_csv(class_012_path, sep='\t', names=names, usecols=indexes_to_use)
        if class_012_df.empty:
            class_012_df = selected_indexes_012_df
        else:
            class_012_df = class_012_df.join(selected_indexes_012_df)
    # validate the number of columns is the number of window indexes
    assert class_012_df.shape[1] == len(window_indexes)
    print(f'{class_012_df.shape[1]} sites in window')
    return class_012_df
