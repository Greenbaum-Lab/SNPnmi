
# given chr name and class split the sites of the class in the chr to files correspanding to windows.
# (in the next step we will merge per class and window the files from all chrs, generating a single file per window.)
# the way this is done is using the output of the previous stpe (prepare_for_split_to_windows):
# we go over the 012 file of the given chr and class.
# each line contains data per individual.
# we read line by line, and for each site, we look in the dictionary from the previous step, to which window id it should be writen.
import os
import pickle
import sys
import json
import time

from os.path import dirname, abspath

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.similarity_helper import file012_to_numpy, numpy_to_file012
from utils.common import get_paths_helper, AlleleClass, args_parser, Cls
from utils.loader import Timer, Loader
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


def split_chr_class_to_windows(options):
    allele_class, chr_short_name, max_site_index, max_window_id, path_helper, site_index_2_window_id = \
        pre_split_chr_class_to_windows(options)

    # Open the files
    window_per_class_and_chr_template = path_helper.window_by_class_and_chr_template
    windows_files = [gzip.open(
        window_per_class_and_chr_template.format(class_name=allele_class.class_name, chr_name=chr_short_name,
                                                 window_id=i), 'wb')
        for i in range(max_window_id + 1)]
    input_file = path_helper.class_by_chr_template.format(class_name=allele_class.class_name, chr_name=chr_short_name)

    # We go over the input file, line by line. Each line is the genetic data of an individual.
    # We skip the first entry, as it is the individual id.
    # Per entry, we look its index in the site_index_2_window_id, and we append the value to the specific window id string.
    # After we are done, we go over the files, and replace the last space with a new-line before we close it, preparing it for the next individual.
    # the input is 012 format, non gzip

    # Estimation for mac 2 chr 1, where we have ~73M sites: it takes about 70 seconds per individual, which means about 18 hours.
    with open(input_file, 'r') as f:
        line = f.readline()
        line_index = 0
        while line:
            per_window_values = [[] for i in range(max_window_id + 1)]
            if line_index % 100 == 0:
                print(f'{time.strftime("%X %x")} line_index {line_index} in file {input_file}')
            # for the given individual, go over the sites, and write them to the designated window (skip the first index which is the individual id)
            sites_only = islice(line.split('\t'), 1, None)
            for site_index, value in enumerate(sites_only):
                window_id = site_index_2_window_id[site_index]
                per_window_values[window_id].extend([value])
            assert site_index == max_site_index
            line_index += 1
            line = f.readline()
            _write_values_to_windows(per_window_values, windows_files)

    return True


def pre_split_chr_class_to_windows(options):
    dataset_name = options.dataset_name
    chr_short_name, mac_maf, class_value = options.args
    class_value = int(class_value)
    cls = Cls(mac_maf, class_value)
    path_helper = get_paths_helper(dataset_name)
    chr_windows_indexes_file = path_helper.windows_indexes_template.format(class_name=cls.name, chr_name=chr_short_name)
    with open(chr_windows_indexes_file, 'rb') as f:
        site_index_2_window_id = pickle.load(f)
    min_site_index = min(site_index_2_window_id.keys())
    max_site_index = max([int(site) for site in site_index_2_window_id.keys()])
    assert int(min_site_index) == 0, f'site indexes must be zero based, but the min index found is {min_site_index}'
    max_window_id = max(site_index_2_window_id.values())
    window_per_class_and_chr_template = path_helper.window_by_class_and_chr_template
    # Generate the folder
    window_per_class_and_chr_sample = window_per_class_and_chr_template.format(class_name=cls.name,
                                                                               chr_name=chr_short_name, window_id=0)
    os.makedirs(dirname(window_per_class_and_chr_sample), exist_ok=True)
    return cls, chr_short_name, max_site_index, max_window_id, path_helper, site_index_2_window_id


def alternative_split_to_windows(options):
    cls, chr_short_name, max_site_index, max_window_id, path_helper, site_index_2_window_id = \
        pre_split_chr_class_to_windows(options)
    window_per_class_and_chr_template = path_helper.window_by_class_and_chr_np_template
    input_file = path_helper.class_by_chr_template.format(class_name=cls.name, chr_name=chr_short_name)
    assert np.all(np.sort(np.array([int(i) for i in site_index_2_window_id.keys()])) == np.arange(max_site_index + 1))
    with Timer("Reformatting 012 file to numpy array"):
        mat012_transpose = file012_to_numpy(input_file).T
    windows_matrix = {}
    for site_id in range(mat012_transpose.shape[0]):
        site = mat012_transpose[site_id]
        window_id = site_index_2_window_id[site_id]
        if window_id in windows_matrix:
            windows_matrix[window_id] = np.concatenate([windows_matrix[window_id], site.reshape(1, -1)])
        else:
            windows_matrix[window_id] = site.reshape(1, -1)

    for wind_id, window in windows_matrix.items():
        with open(window_per_class_and_chr_template.format(class_name=cls.name, chr_name=chr_short_name,
                                                           window_id=wind_id), 'wb') as window_file:
            np.save(window_file, window.T)


def main(options):
    with Timer(f"split with {options.args}"):
        is_executed, msg = execute_with_checkpoint(alternative_split_to_windows, SCRIPT_NAME, options)
        print(msg)
    return is_executed


if __name__ == '__main__':
    options = args_parser()
    main(options)
