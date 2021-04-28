import os
import json 
import pandas as pd
import random
random.seed(a='42', version=2)


def validate_split_vcf_output_stats_file(split_vcf_output_stats_file, num_ind, min_mac, max_mac, min_maf, max_maf, min_chr, max_chr):
    df = pd.read_csv(split_vcf_output_stats_file)
    df['mac_or_maf'] = df.apply(lambda r : r['mac'] if r['mac']!='-' else r['maf'], axis=1)

    # first validate all data is here
    passed = True
    for chr_i in range(min_chr, max_chr+1):
        for mac in range(min_mac, max_mac+1):
            count = len(df[(df['chr_name_name'] == f'chr{chr_i}') & (df['mac'] == f'{mac}')])
            if count != 1:
                passed = False
                print(f'chr{chr_i}, mac {mac} appears {count} times')
        for maf in range(min_maf, max_maf+1):
            count = len(df[(df['chr_name_name'] == f'chr{chr_i}') & (df['maf'] == f'{maf*1.0/100}')])
            if count!=1:
                passed = False
                print(f'chr{chr_i}, mac {mac} appears {count} times')
    assert passed
    print(f'PASSED - all chrs has all relevant macs and mafs once')  

    # next validate all have the correct num_ind
    passed = True
    for c  in ['num_of_indv_after_filter','indv_num_of_lines','012_num_of_lines','num_of_possible_indv']:
        if len(df[df[c]!=num_ind])!=0:
            passed =False
            print(f'wrong number of ind for column {c}')
            print(df[df[c]!=num_ind][['chr_name_name',c]])
    assert passed
    print(f'PASSED - all entries has the correct num of individuals ({num_ind})')

    # next validate same num_of_possible_sites per chr
    grouped = df.groupby('chr_name_name')['num_of_possible_sites'].agg('nunique').reset_index()
    cond = len(grouped[grouped['num_of_possible_sites']!=1])==0
    if not cond:
        print(f'a chr with different number of num_of_possible_sites is found')
        print(grouped[grouped['num_of_possible_sites']!=1])
        assert cond
    else:
        print('PASSED - all chrs has the same number of possilbe sites')

    # validate the number of sites after filtring is indeed the number of sites in the 012 file
    df['validate012sites'] = (df['num_of_sites_after_filter'] == df['pos_num_of_lines']) & \
                             (df['num_of_sites_after_filter'] == df['012_min_num_of_sites']) & \
                             (df['num_of_sites_after_filter'] == df['012_max_num_of_sites'])
    cond =len(df[~df['validate012sites']])==0
    if not cond:
        print(f'number of sites after filtring doesnt match 012 file')
        print(df[~df['validate012sites']])
        assert cond
    else:
        print('PASSED - number of sites in 012 files matches that of vcftools output')

    # validate per chr and class we have a single line
    chr_class_df = df.groupby(['chr_name_name','mac_or_maf'])['mac'].count().reset_index()
    assert(len(chr_class_df[chr_class_df['mac']!=1])==0)
    print('PASSED - single line per chr and name')


def get_class2sites(split_vcf_output_stats_file):
    # per class, we generate a shuffled list of sites (chr and index(!) not site name)
    # given a window size we split the shuffled list to windows of approximatly this size
    df = pd.read_csv(split_vcf_output_stats_file)
    df['mac_or_maf'] = df.apply(lambda r : r['mac'] if r['mac']!='-' else r['maf'], axis=1)
    class2sites = dict()
    for c in df['mac_or_maf'].unique():
        print('Prepare indexes for class',c)
        all_class_indexes = []
        for i,r in df[df['mac_or_maf']==c].iterrows():
            chr_n = r['chr_name_name'][3:]
            num_sites = r['num_of_sites_after_filter']
            all_class_indexes = all_class_indexes + [f'{chr_n};{i}' for i in range(num_sites)]
        print('List is ready, size is:', len(all_class_indexes),'. Shuffle the list')
        random.shuffle(all_class_indexes)
        class2sites[c] = all_class_indexes
        print('Done with class',c)
        print()
        
    return class2sites

# given a window size and a(shuffled) list of tuples of chr id and index, we split the list to windows, and sort each by chr id, to make the reading of it easy
# assuming win_size=100:
# 1. get modulu (for sure smallar than 100)
# 2. check int divesion (num_windows)
# 3. as we know the minimum class has 93920 elements, we know that num_windows > modulu
# so, in the last <module> windows, add 1
def get_windows_sizes(list_size, win_size=100):
    assert list_size>9900
    mod = list_size%win_size
    num_wid = int(list_size/win_size)
    num_win_exact_win_size = num_wid-mod
    assert num_win_exact_win_size>0
    assert (num_win_exact_win_size*win_size)+(mod*(win_size+1)) == list_size
    return num_win_exact_win_size, mod


def split_to_windows(all_class_indexes, window_size, output_file):
    num_win_desired_size, num_win_size_plus_one = get_windows_sizes(len(all_class_indexes), window_size)
    num_win_desired_size, num_win_size_plus_one
    print(f'{len(all_class_indexes)} to cover')
    print(f'{num_win_desired_size} windows of size {window_size}')
    print(f'{num_win_size_plus_one} windows of size {window_size+1}')
    total_num_of_windows = num_win_desired_size + num_win_size_plus_one
    print(f'{total_num_of_windows} total_num_of_windows')
    # this will be a list of windows
    windows = []
    covered=0
    for i in range(num_win_desired_size):
        # todo - not sure about the sort - this will group by chr, but not internaly sort by index
        window = all_class_indexes[covered:covered+window_size]
        window.sort()
        covered+=window_size
        windows.append(window)
    for i in range(num_win_size_plus_one):
        # todo - not sure about the sort
        window = all_class_indexes[covered:covered+(window_size+1)]
        window.sort()
        covered+=window_size+1
        windows.append(window)
    print(f'{covered} covered')
    assert covered==len(all_class_indexes)
    total_length = 0
    for w in windows:
        total_length+=len(w)
    assert total_length==covered

    with open(output_file, "w" ) as f: 
        json.dump(windows, f )
    return total_num_of_windows

def build_windows_indexes_files(split_vcf_output_stats_file,window_size,output_folder, num_ind, min_mac, max_mac, min_maf, max_maf, min_chr, max_chr):
    validate_split_vcf_output_stats_file(split_vcf_output_stats_file, num_ind, min_mac, max_mac, min_maf, max_maf, min_chr, max_chr)
    class2sites = get_class2sites(split_vcf_output_stats_file)
    os.makedirs(output_folder, exist_ok=True)
    for c, class_indexes in class2sites.items():
        print(f'Generate windows indexes file for class {c}')
        output_file=output_folder+'windows_indexes_for_class_'+c+'.json'
        total_num_of_windows = split_to_windows(class_indexes, window_size, output_file)
        with open(output_folder+'number_of_windows_per_class.txt','a+') as f:
            f.write(f'{c} {total_num_of_windows}\n')



#HGDP
# TODO use paths_helper
window_size=100
split_vcf_output_stats_file = r'/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/split_vcfs/split_vcf_output_stats.csv'
output_folder=r'/vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/windows/indexes/'
# TODO consume from config
num_ind = 929
min_mac = 2
max_mac = 18
min_maf = 1
max_maf = 49
min_chr = 1
max_chr = 22
build_windows_indexes_files(split_vcf_output_stats_file,window_size,output_folder, num_ind, min_mac, max_mac, min_maf, max_maf, min_chr, max_chr)