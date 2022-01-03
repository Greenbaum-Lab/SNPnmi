# given chr(vcf name), class and window_size, build a mapping of chr+index to window_index, so that each window size is ~window_size.
# the output is a file per chr with a dict of index (not site name) to window_index
# per class, we generate a shuffled list of sites (chr and index(!) not site name)
# given a window size we split the shuffled list to windows of approximatly this size
# running on mac 2, where we have about 7.3M sites, it takes around 5 minutes.
import pickle
import sys
import pandas as pd
import random
import time
random.seed(a='42', version=2)
from os.path import dirname, abspath
root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Timer
from utils.common import get_paths_helper, AlleleClass, str_for_timer, args_parser
from utils.config import *
from utils.checkpoint_helper import *


SCRIPT_NAME = os.path.basename(__file__)

# TODO - I think this can be refactored and moved to a util file (vcf_helper?)
def get_num_of_sites(dataset_name, chr_short_name, mac_maf, class_value):
    path_helper = get_paths_helper(dataset_name)
    df = pd.read_csv(path_helper.split_vcf_stats_csv_path)
    print(path_helper.split_vcf_stats_csv_path)
    if 'maf' in mac_maf:
        class_value = class_value/100.0
    return df[(df['chr_name'] == chr_short_name) & (df[mac_maf] == class_value)]['num_of_sites_after_filter'].values[0]

def get_num_of_sites_per_chr(dataset_name, mac_maf, class_value):
    chr_short_names = get_dataset_vcf_files_short_names(dataset_name)
    chr_2_num_of_sites = dict()
    for chr_short_name in chr_short_names:
        chr_2_num_of_sites[chr_short_name] = get_num_of_sites(dataset_name, chr_short_name, mac_maf, class_value)
    return chr_2_num_of_sites

# given a window size and a(shuffled) list of tuples of chr id and index, we split the list to windows, and sort each by chr id, to make the reading of it easy
# assuming win_size=100:
# 1. get modulu (for sure smallar than 100)
# 2. check int divesion (num_windows)
# 3. (as we know the minimum class has 93920 elements, we know that num_windows > modulu)
# so, in the last <module> windows, add 1
def get_windows_sizes(list_size, window_size):
    num_large_windows = list_size % window_size
    num_wid = int(list_size/window_size)
    assert num_wid > num_large_windows
    num_win_exact_win_size = num_wid - num_large_windows
    assert num_win_exact_win_size >= 0
    assert (num_win_exact_win_size * window_size) + (num_large_windows * (window_size + 1)) == list_size
    return num_win_exact_win_size, num_large_windows

# given the number of sites and the disered window_size, create random windows of the given size or the size +1.
# for example, if we have 105 sites, and the desired window size is 10, we will have 5 windows with 10 sites, and 5 with 11.
# returns a mapping of chr to site index to window id
def split_to_windows(chr_2_num_of_sites, window_size):
    num_of_sites = sum(chr_2_num_of_sites.values())
    num_win_desired_size, num_win_size_plus_one = get_windows_sizes(num_of_sites, window_size)
    print(f'{num_of_sites} to cover')
    print(f'{num_win_desired_size} windows of size {window_size}')
    print(f'{num_win_size_plus_one} windows of size {window_size+1}')
    total_num_of_windows = num_win_desired_size + num_win_size_plus_one
    print(f'{total_num_of_windows} total_num_of_windows')
    assert window_size*num_win_desired_size + (window_size+1)*num_win_size_plus_one == num_of_sites

    # note we use a fixed seed, so will always create the same output for reproducing the list.
    shuffled_sites_indexes = []
    for chr_name, chr_num_sites in chr_2_num_of_sites.items():
         shuffled_sites_indexes += [f'{chr_name};{i}' for i in range(chr_num_sites)]
    random.shuffle(shuffled_sites_indexes)
    chr_2_index_2_window_id = dict()
    covered = 0
    for window_id in range(num_win_desired_size):
        window = shuffled_sites_indexes[covered:covered+window_size]
        window.sort()
        for i in window:
            chr_name, site_index = i.split(';')
            if not chr_name in chr_2_index_2_window_id.keys():
                chr_2_index_2_window_id[str(chr_name)] = dict()
            # need to convert to int so json will output without any numpy issues
            chr_2_index_2_window_id[str(chr_name)][int(site_index)] = int(window_id)
        covered += window_size
    for window_id in range(num_win_size_plus_one):
        window = shuffled_sites_indexes[covered:covered+(window_size+1)]
        window.sort()
        for i in window:
            chr_name, site_index = i.split(';')
            if not chr_name in chr_2_index_2_window_id.keys():
                chr_2_index_2_window_id[str(chr_name)] = dict()
            # need to convert to int so json will output without any numpy issues
            chr_2_index_2_window_id[str(chr_name)][int(site_index)] = int(num_win_desired_size + window_id)
        covered += window_size+1
    print(f'{covered} covered')
    assert covered == num_of_sites

    assert sum([len(index_2_window_id.keys()) for index_2_window_id in chr_2_index_2_window_id.values()]) == covered
    return chr_2_index_2_window_id, total_num_of_windows

def validate_windows(chr_2_index_2_window_id, chr_2_num_of_sites, window_size):
    # we need to validate that we mapped all sites, and that each window size is either window_size or window_size+1.
    # First we validate the nubmer of keys is num_of_sites
    num_of_sites = sum(chr_2_num_of_sites.values())
    assert sum([len(index_2_window_id.keys()) for index_2_window_id in chr_2_index_2_window_id.values()]) == num_of_sites
    # Second, lets validate that per chr we covered all sites
    # make sure keys are the same
    assert set(chr_2_index_2_window_id.keys()) == set(chr_2_num_of_sites.keys())
    # per key, compare sizes
    for chr_name, chr_num_sites in chr_2_num_of_sites.items():
        assert chr_num_sites == len(chr_2_index_2_window_id[chr_name].keys())
    
    # Finally, to validate windows sizes, we build a mapping of windows_id to number of sites.
    window_id_2_window_size = dict()
    for index_2_window_id in chr_2_index_2_window_id.values():
        for k,v in index_2_window_id.items():
            if not v in window_id_2_window_size.keys():
                window_id_2_window_size[v] = 0
            window_id_2_window_size[v] += 1
    # Now we can validate that all sizes are either window_size or window_size+1, and that the sum is num_of_sites
    sum_window_sizes = 0
    for window_id, specific_window_size in window_id_2_window_size.items():
        assert specific_window_size - window_size <= 1, f'window id {window_id} has window size of {specific_window_size}, which is >1 w.r.t expected window size {window_size}'
        sum_window_sizes += specific_window_size
    assert sum_window_sizes == num_of_sites


def build_windows_indexes_files(options):
    dataset_name = options.dataset_name
    mac_maf, class_value, window_size = options.args
    class_value = int(class_value)
    allele_class = AlleleClass(mac_maf, class_value)
    path_helper = get_paths_helper(dataset_name)

    chr_2_num_of_sites = get_num_of_sites_per_chr(dataset_name, mac_maf, class_value)
    print(f'class {mac_maf}_{class_value}')
    for chr_name in chr_2_num_of_sites.keys():
        print(f'chr {chr_name} has {chr_2_num_of_sites[chr_name]} sites')

    chr_2_index_2_window_id, total_num_of_windows = split_to_windows(chr_2_num_of_sites, window_size)
    validate_windows(chr_2_index_2_window_id, chr_2_num_of_sites, window_size)

    # log number of windows to file for future use
    os.makedirs(path_helper.windows_per_class_folder.format(class_name=allele_class.class_name),
                exist_ok=True)
    with open(path_helper.number_of_windows_per_class_template.format(class_name=allele_class.class_name), 'w')\
            as number_of_windows_per_class_file:
        number_of_windows_per_class_file.write(str(total_num_of_windows))

    for chr_short_name, index_2_window_id in chr_2_index_2_window_id.items():
        output_file = path_helper.windows_indexes_template.format(class_name=allele_class.class_name, chr_name=chr_short_name)
        os.makedirs(dirname(output_file), exist_ok=True)
        with open(output_file, "wb") as f:
            pickle.dump(index_2_window_id, f)
    return True


def main(options):
    with Timer(f"Prepare for split to windows with {str_for_timer(options)}"):
        is_executed, msg = execute_with_checkpoint(build_windows_indexes_files, SCRIPT_NAME, options)
    return is_executed


if __name__ == '__main__':
    arguments = args_parser()
    main(arguments)

