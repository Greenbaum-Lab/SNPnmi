# python3 validate_calc_distances_in_windows.py 2 18 1 49

import sys
import os
from os.path import dirname, abspath

root_path = dirname(dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

import gzip
from utils.common import get_paths_helper, args_parser
from utils.common import get_number_of_windows_by_class
from utils.config import get_num_individuals


def go_over_classes(mac_maf, classes_names, paths_helper, class2num_windows):
    class2missing = dict()
    for class_name in classes_names:
        win_dir = paths_helper.windows_folder + f'{mac_maf}_{class_name}/'
        validated_dir = f'{win_dir}/validated_count_dist/'
        validated_flag_template = validated_dir + '/validated_count_dist_window_{i}.txt'

        num_windows = class2num_windows[str(class_name)]
        missing = []
        for i in range(num_windows):
            if i % 500 == 0:
                print(f'class {class_name}: done {i}/{num_windows}')
            validated_flag = validated_flag_template.format(i=i)
            if not os.path.isfile(validated_flag):
                missing.append(i)
                print(f'File is missing: {validated_flag}')
        print(f'done {mac_maf} {class_name}')
        print(f'missing {len(missing)}')
        print(missing)
        if len(missing) > 0:
            class2missing[class_name] = missing
    return class2missing


def main(options):
    print('Number of arguments:', len(options.args), 'arguments.')
    print('Argument List:', str(options.args))
    min_mac = int(options.args[0])
    assert min_mac >= 0
    max_mac = int(options.args[1])
    assert max_mac >= 0
    min_maf = int(options.args[2])
    assert min_maf >= 0
    max_maf = int(options.args[3])
    assert max_maf >= 0

    print('min_mac', min_mac)
    print('max_mac', max_mac)
    print('min_maf', min_maf)
    print('max_maf', max_maf)

    paths_helper = get_paths_helper(options.dataset_name)
    class2num_windows = get_number_of_windows_by_class(paths_helper.number_of_windows_per_class_path)
    mac_class2missing = go_over_classes('mac', range(min_mac, max_mac + 1), paths_helper, class2num_windows)

    maf_classes_names = [str(maf / 100) for maf in range(min_maf, max_maf + 1)]
    maf_class2missing = go_over_classes('maf', maf_classes_names, paths_helper, class2num_windows)
    print(mac_class2missing)
    print(maf_class2missing)


if __name__ == "__main__":
    run_arguments = args_parser()
    main(run_arguments)
