# python3 validate_calc_distances_in_windows.py 2 18 1 49

import sys
import os
from os.path import dirname, abspath
root_path = dirname(dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

import gzip
from utils.common import get_paths_helper
from utils.common import get_number_of_windows_by_class
from utils.config import get_num_individuals


def file_len(fname):
    with gzip.open(fname,'rb') as f:
        for i, l in enumerate(f):
            pass
    return i + 1

def go_over_classes(mac_maf, classes_names, paths_helper, class2num_windows):
    total_len = len(classes_names)
    for class_name in classes_names:
        win_dir = paths_helper.windows_folder + f'{mac_maf}_{class_name}/'
        num_windows = class2num_windows[str(class_name)]
        for i in range(num_windows):
            count_dist_file = f'{win_dir}count_dist_window_{i}.tsv.gz'
            print(f'Assert len of {count_dist_file} / {total_len}')
            assert file_len(count_dist_file) == get_num_individuals()-1

def main(args):
    print ('Number of arguments:', len(args), 'arguments.')
    print ('Argument List:', str(args))
    min_mac = int(args[0])
    assert min_mac >= 0
    max_mac = int(args[1])
    assert max_mac >= 0
    min_maf = int(args[2])
    assert min_maf >= 0
    max_maf = int(args[3])
    assert max_maf >= 0

    print('min_mac',min_mac)
    print('max_mac',max_mac)
    print('min_maf',min_maf)
    print('max_maf',max_maf)

    paths_helper = get_paths_helper()
    class2num_windows = get_number_of_windows_by_class(paths_helper.number_of_windows_per_class_path)

    go_over_classes('mac', range(min_mac, max_mac), paths_helper, class2num_windows)

    maf_classes_names = [str(maf/100) for maf in range(min_maf, max_maf+1)]
    go_over_classes('maf', maf_classes_names, paths_helper, class2num_windows)

if __name__ == "__main__":
   main(sys.argv[1:])
