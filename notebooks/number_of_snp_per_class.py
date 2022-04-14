#!/usr/bin/env python
import json
import sys
from os.path import dirname, abspath
import matplotlib.pyplot as plt
import numpy as np

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.common import args_parser, get_paths_helper, class_iter

options = args_parser()
paths_helper = get_paths_helper(options.dataset_name)


mac_min_range, mac_max_range = options.mac
maf_min_range, maf_max_range = options.maf
window_to_snps_path = paths_helper.windows_dir + 'number_of_windows_per_class.txt'
window_size_path = paths_helper.windows_dir + 'window_size.txt'
mac_class_names = np.arange(mac_min_range, mac_max_range + 1) if options.dataset_name != 'arabidopsis' else np.arange(
    mac_min_range, mac_max_range + 1, 2)
maf_class_names = np.arange(maf_min_range, maf_max_range + 1) / 100


with open(window_to_snps_path,'r') as f:
    windows2count = json.load(f)
with open(window_size_path, 'r') as f:
    window_size = int(f.readline())

num_of_snps = {'mac': [], 'maf': []}
for cls in class_iter(options):
    num_of_snps[cls.mac_maf].append(int(windows2count[cls.name]) * window_size)
for mac_maf in ["mac", "maf"]:
    class_names = mac_class_names if mac_maf == ' maf' else maf_class_names
    plt.plot(class_names, num_of_snps[mac_maf])
    plt.yscale('log')
    plt.xlabel(f"{mac_maf}", fontsize=16)
    plt.xticks(fontsize=10)
    plt.yticks(fontsize=10)
    plt.title('site frequency spectrum (SFS)', fontsize=18)
    plt.savefig(f'{paths_helper.summary_dir}num_of_snps/{mac_maf}.svg')
    plt.clf()


