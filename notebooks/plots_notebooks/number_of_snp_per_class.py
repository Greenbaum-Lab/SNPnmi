#!/usr/bin/env python



import json
import sys
from os.path import dirname, abspath
import matplotlib.pyplot as plt
import numpy as np

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.common import args_parser, get_paths_helper, is_class_valid

options = args_parser()
paths_helper = get_paths_helper(options.dataset_name)


mac_min_range, mac_max_range = options.mac
maf_min_range, maf_max_range = options.maf
window_to_snps_path = paths_helper.windows_dir + 'number_of_windows_per_class.txt'
window_size_path = paths_helper.windows_dir + 'window_size.txt'
mac_class_names = np.arange(mac_min_range, mac_max_range + 1) if options.dataset_name != 'arabidopsis' else np.arange(
    mac_min_range, mac_max_range + 1, 2)
maf_class_names = np.arange(maf_min_range, maf_max_range + 1) / 100


with open(window_to_snps_path) as f:
    windows2count = json.load(f)
with open(window_size_path, 'r') as f:
    window_size = int(f.readline())

for mac_maf in ['mac', 'maf']:
    num_of_snps = []
    is_mac = mac_maf == 'mac'
    min_range = mac_min_range if is_mac else maf_min_range
    max_range = mac_max_range if is_mac else maf_max_range
    for val in range(min_range, max_range+1):
        if not is_class_valid(options, mac_maf, val):
            continue
        # in maf we take 0.x
        if not is_mac:
            val = f'{val * 1.0/100}'
        class_name = f'{mac_maf}_{val}'
        num_of_snps.append(int(windows2count[class_name]) * window_size)
    class_names = mac_class_names if is_mac else maf_class_names
    plt.plot(class_names, num_of_snps)
    plt.yscale('log')
    plt.xlabel(f"{mac_maf}")
    plt.title('Num of SNPs per class')
    plt.savefig(f'{paths_helper.summary_dir}num_of_snps/{mac_maf}.svg')
    plt.clf()


