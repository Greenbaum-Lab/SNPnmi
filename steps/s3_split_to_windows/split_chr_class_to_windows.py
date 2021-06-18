DEBUG = False
# given chr name and class split the sites of the class in the chr to files correspanding to windows.
# in the next step we will merge per class and window the files from all chrs, generating a single file per window.
# the way this is done is using the output of the previous stpe (prepare_for_split_to_windows):
# we go over the 012 file of the given char and class.
# each line contains data per individual.
# we read line by line, and for each site, we look in the dictionary from the previous step, to which window id it should be writen.
import os
import sys
import json
import time
from os.path import dirname, abspath
root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)
from utils.common import get_paths_helper, AlleleClass
from utils.config import *
from utils.checkpoint_helper import *
import gzip
from itertools import islice

SCRIPT_NAME = os.path.basename(__file__)


def _write_values_to_windows(per_window_values, windows_files):
    for window_index, window_file in enumerate(windows_files):
        values = per_window_values[window_index]
        to_write = '\t'.join(values) + '\n'
        window_file.write(to_write.encode())

def split_chr_class_to_windows(dataset_name, chr_short_name, mac_maf, class_value):
    allele_class = AlleleClass(mac_maf, class_value)
    path_helper = get_paths_helper(dataset_name)
    chr_windows_indexes_file = path_helper.windows_indexes_template.format(class_name = allele_class.class_name, chr_name=chr_short_name)
    site_index_2_window_id = json.load(open(chr_windows_indexes_file,'r'))

    min_site_index = min(site_index_2_window_id.keys())
    max_site_index = max([int(site) for site in site_index_2_window_id.keys()])
    assert int(min_site_index) == 0, f'site indexes must be zero based, but the min index found is {min_site_index}'

    max_window_id = max(site_index_2_window_id.values())
    min_window_id = min(site_index_2_window_id.values())
    # we assume zero based windows ids
    assert min_window_id == 0, f'windows ids must be zero based, but the min index found is {min_window_id}'

    # TODO - we are potentialy opening a lot of files here. Need to try this for mac 2 chr 1.
    window_per_class_and_chr_template = path_helper.windows_per_class_and_chr_template

    # Generate the folder
    window_per_class_and_chr_sample = window_per_class_and_chr_template.format(class_name = allele_class.class_name, chr_name=chr_short_name, window_id=0)
    os.makedirs(dirname(window_per_class_and_chr_sample), exist_ok=True)

    # Open the files
    windows_files = [gzip.open(window_per_class_and_chr_template.format(class_name = allele_class.class_name, chr_name=chr_short_name, window_id=i), 'wb') 
                     for i in range(max_window_id + 1)]
    input_file = path_helper.class_by_chr_template.format(class_name = allele_class.class_name, chr_name=chr_short_name)

    # We go over the input file, line by line. Each line is the genetic data of an individual.
    # We skip the first entry, as it is the individual id.
    # Per entry, we look its index in the site_index_2_window_id, and we append the value to the specific window id string.
    # After we are done, we go over the files, and replace the last space with a new-line before we close it, preparing it for the next individual.
    # the input is 012 format, non gzip

    # Estimation for mac 2 chr 1, where we have ~73M sites: it takes about 70 seconds per individual, which means about 18 hours.
    with open(input_file, 'r') as f:
        line =  f.readline()
        line_index = 0
        while line:
            per_window_values = [[] for i in range(max_window_id + 1)]
            if line_index%100 == 0:
                print(f'{time.strftime("%X %x")} line_index {line_index} in file {input_file}')
            # for the given individual, go over the sites, and write them to the designated window (skip the first index which is the individual id)
            sites_only = islice(line.split('\t'), 1, None)
            for site_index, value in  enumerate(sites_only):
                window_id = site_index_2_window_id[str(site_index)]
                per_window_values[window_id].extend([value])
            assert site_index == max_site_index
            line_index += 1
            line = f.readline()
            _write_values_to_windows(per_window_values, windows_files)

    return True

def main(args):
    s = time.time()
    dataset_name = args[0]
    is_executed, msg = execute_with_checkpoint(split_chr_class_to_windows, SCRIPT_NAME, dataset_name, args)
    print(f'{msg}. {(time.time()-s)/60} minutes total run time')
    return is_executed

def _test_me():
    dataset_name = 'hgdp_test'
    chr_short_name = 'chr22'
    mac_maf = 'maf'
    class_value = 1
    split_chr_class_to_windows(dataset_name, chr_short_name, mac_maf, class_value)

if DEBUG:
    _test_me()
elif __name__ == '__main__':
    main(sys.argv[1:])

