# specific input 012 file:
# python3 calc_distances_in_window.py mac 2 0 1 -1 -1 2 2 True 1000 1000
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

from utils.common import get_number_of_windows_by_class, build_empty_upper_left_matrix, write_upper_left_matrix_to_file, get_paths_helper, write_pairwise_distances

OUTPUT_PATTERN_DIST_FILE = 'count_dist_window_{window_index}.tsv.gz'

# PARAMS
# UTILS FOR PARAMS
# if we have less than this which are valid (not -1), site is not included in calc.
min_valid_sites_precentage = 0.1


# mac_maf = 'maf'
# class_name = '0.49'
# min_window_index = 0
# max_window_index = 3
# min_minor_freq_expected = 0.49
# max_minor_freq_expected = 0.5
# min_minor_count_expected = -1
# max_minor_count_expected = -1
# python3 calc_distances_in_window.py mac 0.49 0 4 0.49 0.5 -1 -1



def get_window(class_012_path_template, windows_indexes_path, mac_maf, class_name, window_index):
    # read indexes of window
    with open(windows_indexes_path) as f:
        windows_indexes = json.load(f)
    window_indexes = windows_indexes[window_index]
    print(f'There are {len(window_indexes)} indexes in window indexes list')
    chr_id2indexes = dict()
    print('Read sites from appropriate chrs')
    for chr_id_index in window_indexes:
        chr_id, index = chr_id_index.split(';',1)
        # if chr_id not in the dict, we get an empty list
        indexes = chr_id2indexes.get(chr_id, [])
        indexes.append(int(index))
        chr_id2indexes[chr_id] = indexes
    # no need to sort as we supply this list to pandas
    # sort the indexes so that we can easily read the file
    #{k:v.sort() for k, v in chr_id2indexes.items()}

    # go over the chr_ids, and get from the file the correct columns
    class_012_df = pd.DataFrame()
    for chr_id, indexes in chr_id2indexes.items():
        class_name_for_012 = class_name
        if mac_maf=='maf':
            class_name_for_012 = f'{float(class_name):.2f}'
        class_012_path = class_012_path_template.format(chr_id = chr_id, mac_maf = mac_maf, class_name = class_name_for_012)
        # get number of columns in chr:
        with open(class_012_path,'r') as f:
            num_columns = len(f.readline().split('\t'))
        #print(f'{len(indexes)} / {num_columns-1} sites will be used from file {class_012_path}')
        names = [f'chr{chr_id}_idx{i}' for i in range(num_columns)]
        # we add one as the csv contains the individual id in the first index
        indexes_to_use = [i+1 for i in indexes]
        # read file to pandas df
        selected_indexes_012_df = pd.read_csv(class_012_path, sep='\t', names= names, usecols = indexes_to_use)
        if class_012_df.empty:
            class_012_df = selected_indexes_012_df
        else:
            class_012_df = class_012_df.join(selected_indexes_012_df)
    # validate the number of columns is the number of window indexes
    assert class_012_df.shape[1] == len(window_indexes)
    print(f'{class_012_df.shape[1]} sites in window')
    return class_012_df

def get_012_df(input_012_path):
    # get number of columns in chr:
    with gzip.open(input_012_path,'rb') as f:
        num_columns = len(f.readline().decode().split('\t'))
    #print(f'{len(indexes)} / {num_columns-1} sites will be used from file {class_012_path}')
    names = [f'idx{i}' for i in range(num_columns)]
    # read file to pandas df
    # we drop the first column as the csv contains the individual id in the first index
    df = pd.read_csv(input_012_path, sep='\t', names= names, compression='gzip')
    return df.iloc[:, 1:]

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
        site_calc_pairwise_distances_with_guardrails(window_df, site_index, min_valid_sites_precentage, min_minor_freq_expected, max_minor_freq_expected, min_minor_count_expected, max_minor_count_expected, window_pairwise_counts, window_pairwise_dist)
    return window_pairwise_counts, window_pairwise_dist

def _check_guardrails(num_individuals, num_valid_genotypes, ref_count, non_ref_count, min_valid_sites_precentage, min_minor_freq_expected, max_minor_freq_expected, min_minor_count_expected, max_minor_count_expected):
    #print(f'Check guardrails')
    # guardrail #1 - min_valid_sites_precentage
    percentage_valid_sites = float(num_valid_genotypes)/num_individuals
    #print(f'Precentage of valid sites: {percentage_valid_sites}')
    if percentage_valid_sites < min_valid_sites_precentage:
        #print(f'ERROR: % of valid sites is {percentage_valid_sites}, lower than allowd: {min_valid_sites_precentage}.')
        assert percentage_valid_sites < min_valid_sites_precentage

    # guardrail #2 mac/maf validation
    if (min_minor_freq_expected==-1 or max_minor_freq_expected==-1) and (min_minor_count_expected==-1 or max_minor_count_expected==-1):
        #print(f'ERROR: min_minor_freq_expected, max_minor_freq_expected or min_minor_count_expected, max_minor_count_expected must be >-1')
        assert (min_minor_freq_expected==-1 or max_minor_freq_expected==-1) and (min_minor_count_expected==-1 or max_minor_count_expected==-1)
    
    #if maf: validate min_minor_freq_expected and max_minor_freq_expected
    if min_minor_freq_expected>-1 and max_minor_freq_expected>-1:
        minor_count = min(ref_count, non_ref_count)
        minor_freq = float(minor_count)/(2*num_valid_genotypes)
        #print(f'Minor allele frequency: {minor_freq}')
        if minor_freq < min_minor_freq_expected:
            #print(f'ERROR: minor frequency is too low - {minor_freq}, allowd: {min_minor_freq_expected}.')
            assert minor_freq < min_minor_freq_expected
        if minor_freq > max_minor_freq_expected:
            #print(f'ERROR: minor frequency is too high - {minor_freq}, allowd: {max_minor_freq_expected}.')
            assert minor_freq > max_minor_freq_expected
    
    #if mac: validate min_minor_count_expected and max_minor_count_expected
    if min_minor_count_expected>-1 and max_minor_count_expected>-1:
        minor_count = min(ref_count, non_ref_count)
        #print(f'Minor allele count: {minor_count}')
        if minor_count < min_minor_count_expected:
            #print(f'ERROR: minor frequency is too low - {minor_freq}, allowd: {min_minor_freq_expected}.')
            assert minor_count < min_minor_count_expected
        if minor_count > max_minor_count_expected:
            #print(f'ERROR: minor frequency is too high - {minor_freq}, allowd: {max_minor_freq_expected}.')
            assert minor_count > max_minor_count_expected
    #print(f'Passed guardrails')

def site_calc_pairwise_distances_with_guardrails(window_df, site_index, min_valid_sites_precentage, min_minor_freq_expected, max_minor_freq_expected, min_minor_count_expected, max_minor_count_expected, window_pairwise_counts, window_pairwise_dist):
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
    _check_guardrails(num_individuals, num_valid_genotypes, ref_count, non_ref_count, min_valid_sites_precentage, min_minor_freq_expected, max_minor_freq_expected, min_minor_count_expected, max_minor_count_expected)
    site_calc_pairwise_distances(genotypes, num_individuals, ref_freq, non_ref_freq, window_pairwise_counts, window_pairwise_dist)

def _calc_dist(i1_val, i2_val, ref_freq, non_ref_freq):
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
    dist = ((1-non_ref_freq)*(i1_val*i2_val) + (1-ref_freq)*((2-i1_val)*(2-i2_val)))/4
    assert dist>=0, dist
    assert dist<=1, dist
    return dist

def _calc_dist_directly(i1_val, i2_val, ref_freq, non_ref_freq):
    # set a,b
    # both ref
    if i1_val == 0:
        a='ref'
        b='ref'
    if i1_val == 1:
        a='non_ref'
        b='ref'
    # both non ref
    if i1_val == 2:
        a='non_ref'
        b='non_ref'
    # set c,d
    if i2_val == 0:
        c='ref'
        d='ref'
    if i2_val == 1:
        c='non_ref'
        d='ref'
    if i2_val == 2:
        c='non_ref'
        d='non_ref'
    Iac = 1 if a==c else 0
    Iad = 1 if a==d else 0
    Ibc = 1 if b==c else 0
    Ibd = 1 if b==d else 0
    f_a = ref_freq if a=='ref' else non_ref_freq
    f_b = ref_freq if b=='ref' else non_ref_freq
    dist = (((1-f_a)*(Iac+Iad)) + ((1-f_b)*(Ibc+Ibd)))/4.0
    assert dist>=0, dist
    assert dist<=1, dist
    return dist

def site_calc_pairwise_distances(genotypes, num_individuals, ref_freq, non_ref_freq, window_pairwise_counts, window_pairwise_dist):
    for i1 in range(num_individuals-1):
        i1_val = genotypes[i1]
        # if this entry is not valid for i1, no need to go over all the others, nothing to add to freq nor counts
        #if i1%100==0:
        #    print(f'Done with individual {i1}/{len(genotypes)}')
        if i1_val == -1:
            continue
        for i2 in range(i1+1, num_individuals):
            i2_val = genotypes[i2]
            if i2_val == -1:
                continue
            else:            
                # this is a valid entry, we add 1 to the count
                window_pairwise_counts[i1][i2-i1-1] += 1
                window_pairwise_dist[i1][i2-i1-1] += _calc_dist(i1_val, i2_val, ref_freq, non_ref_freq)

def calc_distances_in_windows(
    class_012_path_template,
    windows_indexes_path,
    mac_maf,
    class_name,
    min_window_index,
    max_window_index,
    output_dir,
    min_valid_sites_precentage,
    min_minor_freq_expected,
    max_minor_freq_expected,
    min_minor_count_expected,
    max_minor_count_expected,
    # we can either sample the file by min and max window indexes, or use an entire 012 file
    use_specific_012_file,
    input_012_template=None,
    min_input_012_index=-1,
    max_input_012_index=-1):

    os.makedirs(output_dir, exist_ok=True)
    if use_specific_012_file:
        start_time = time.time()
        for input_012_index in range(min_input_012_index, max_input_012_index + 1):
            input_012_file = input_012_template.format(input_012_index=input_012_index)
            print(f'process {input_012_file}')
            window_df = get_012_df(input_012_file)
            output_count_dist_file = output_dir + OUTPUT_PATTERN_DIST_FILE.format(window_index=input_012_index)

            window_pairwise_counts, window_pairwise_dist = window_calc_pairwise_distances_with_guardrails(
                window_df,
                min_valid_sites_precentage,
                min_minor_freq_expected,
                max_minor_freq_expected,
                min_minor_count_expected,
                max_minor_count_expected)

            print(f'output distances file to {output_count_dist_file}')
            write_pairwise_distances(output_count_dist_file, window_pairwise_counts, window_pairwise_dist)
            print(f'{(time.time()-start_time)/60} minutes for class: {mac_maf}_{class_name}, input_012_index: {input_012_index}')

    else:
        print(f'Class: {mac_maf}_{class_name}, window indexes: [{min_window_index} , {max_window_index})')
        for window_index in range(min_window_index, max_window_index):
            output_count_dist_file = output_dir + OUTPUT_PATTERN_DIST_FILE.format(window_index=window_index)
            if os.path.isfile(output_count_dist_file):
                print(f'Window file exist, do not calc! {output_count_dist_file}')
                continue
            start_time = time.time()
            print(f'Class: {mac_maf}_{class_name}, window index: {window_index}')
            window_df = get_window(class_012_path_template, windows_indexes_path, mac_maf, class_name, window_index)
            # 100 indexes in a window takes ~5 minutes 
            # 100 windows will take about 9 hours
            # using percision of 5 decimals will generates a file of ~1600KB in gz format
            # we have 322,483 windows of 100 indexes each
            # will take over 3 years to process on one machine
            # will generate about 515 GB of data
            window_pairwise_counts, window_pairwise_dist = window_calc_pairwise_distances_with_guardrails(
                window_df,
                min_valid_sites_precentage,
                min_minor_freq_expected,
                max_minor_freq_expected,
                min_minor_count_expected,
                max_minor_count_expected)

            print(f'output distances file to {output_count_dist_file}')
            write_pairwise_distances(output_count_dist_file, window_pairwise_counts, window_pairwise_dist)
            print(f'{(time.time()-start_time)/60} minutes for class: {mac_maf}_{class_name}, window index: {window_index}')

def main(args):
    s = time.time()
    print ('Number of arguments:', len(args), 'arguments.')
    print ('Argument List:', str(args))
    mac_maf = args[0]
    assert mac_maf=='mac' or mac_maf=='maf'
    class_name = args[1]
    min_window_index = int(args[2])
    max_window_index = int(args[3])
    min_minor_freq_expected = float(args[4])
    assert min_minor_freq_expected>=-1
    assert min_minor_freq_expected<=1
    max_minor_freq_expected = float(args[5])
    assert max_minor_freq_expected>=-1
    assert max_minor_freq_expected<=1
    min_minor_count_expected = int(args[6])
    assert min_minor_count_expected>=-1
    max_minor_count_expected = int(args[7])
    assert max_minor_count_expected>=-1
    use_specific_012_file = False
    min_input_012_index = -1
    max_input_012_index = -1
    if len(args) > 8:
        use_specific_012_file = bool(args[8])
    if use_specific_012_file:
        min_input_012_index = int(args[9])
        max_input_012_index = int(args[10])
    if not use_specific_012_file:
        assert min_window_index>=0
        assert max_window_index>=0
        assert min_window_index<max_window_index

    print('mac_maf',mac_maf)
    print('class_name',class_name)
    print('min_window_index',min_window_index)
    print('max_window_index',max_window_index)
    print('min_minor_freq_expected',min_minor_freq_expected)
    print('max_minor_freq_expected',max_minor_freq_expected)
    print('min_minor_count_expected',min_minor_count_expected)
    print('max_minor_count_expected',max_minor_count_expected)
    print('use_specific_012_file',use_specific_012_file)
    print('min_input_012_index',min_input_012_index)
    print('max_input_012_index',max_input_012_index)

    # Prepare paths
    paths_helper = get_paths_helper()

    # /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/windows/mac_2/transposed/
    classes_folder = paths_helper.classes_folder
    class_012_path_template = classes_folder + r'chr{chr_id}/{mac_maf}_{class_name}.012'
    input_012_template = paths_helper.windows_folder + f'{mac_maf}_{class_name}/window_' + '{input_012_index}.012.tsv.gz'

    windows_indexes_files_folder = classes_folder + r'windows/indexes/'
    windows_indexes_path_template = windows_indexes_files_folder + 'windows_indexes_for_class_{class_name}.json'
    windows_indexes_path = windows_indexes_path_template.format(class_name=class_name)
    output_dir = f'{classes_folder}windows/{mac_maf}_{class_name}/'
    print('windows_indexes_files_folder',windows_indexes_files_folder)
    print('windows_indexes_path',windows_indexes_path)
    print('output_dir',output_dir)

    calc_distances_in_windows(
        class_012_path_template,
        windows_indexes_path,
        mac_maf,
        class_name,
        min_window_index,
        max_window_index,
        output_dir,
        min_valid_sites_precentage,
        min_minor_freq_expected,
        max_minor_freq_expected,
        min_minor_count_expected,
        max_minor_count_expected,
        use_specific_012_file,
        input_012_template,
        min_input_012_index,
        max_input_012_index)

    print(f'{(time.time()-s)/60} minutes total run time')

# mac_maf = 'mac'
# class_name = '2'
# min_window_index = 0
# max_window_index = 1
# min_minor_freq_expected = -1
# max_minor_freq_expected = -1
# min_minor_count_expected = 2
# max_minor_count_expected = 2
# main([mac_maf, class_name, min_window_index, max_window_index, min_minor_freq_expected, max_minor_freq_expected, min_minor_count_expected, max_minor_count_expected])

if __name__ == "__main__":
   main(sys.argv[1:])
