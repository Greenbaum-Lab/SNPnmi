#!/usr/bin/env python
import sys
from os.path import dirname, abspath
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.common import args_parser, get_paths_helper, is_class_valid

options = args_parser()
paths_helper = get_paths_helper(options.dataset_name)
summary_dir = paths_helper.summary_dir
tree_height_matrix = summary_dir + 'tree_heights_per_class_{size}.csv'
SCORES = ['max_height', 'avg_height', 'avg_leaves']

mac_min_range, mac_max_range = options.mac
maf_min_range, maf_max_range = options.maf
SCORE2COLOR_DICT = {'max_height': 'b', 'avg_height': 'r', 'avg_leaves': 'g'}
mac_class_names = np.arange(mac_min_range, mac_max_range + 1) if options.dataset_name != 'arabidopsis' else np.arange(
    mac_min_range, mac_max_range + 1, 2)
maf_class_names = np.arange(maf_min_range, maf_max_range+1) / 100

for num_of_snp in [1000, 5000]:
    f = plt.figure()
    f.set_figwidth(15)
    f.set_figheight(15)
    ax = f.add_subplot(111)
    path = tree_height_matrix.format(size=num_of_snp)
    df = pd.read_csv(path)
    for mac_maf in ['mac', 'maf']:

        is_mac = mac_maf == 'mac'
        class_names = mac_class_names if is_mac else maf_class_names
        for score in SCORES:
            avg = []
            std = []
            min_range = mac_min_range if is_mac else maf_min_range
            max_range = mac_max_range if is_mac else maf_max_range
            if min_range > 0:
                for val in range(min_range, max_range+1):
                    if not is_class_valid(options, mac_maf, val):
                        continue
                    # in maf we take 0.x
                    if not is_mac:
                        val = val * 1.0/100
                    class_name = f'{mac_maf}_{val}'
                    class_values = df[df.Class == class_name]
                    avg.append(float(class_values[f'avg_{score}']))
                    std.append(float(class_values[f'std_{score}']))
            avg = np.array(avg)
            std = np.array(std)
            plt.plot(class_names, avg, color=SCORE2COLOR_DICT[score], label=score)
            plt.fill_between(class_names, y1=avg - std, y2=avg + std, alpha=0.3)
        plt.xlabel(f"{mac_maf}")
        plt.legend(title="Scores", loc='upper left')
        plt.title(f'Structure depth with {num_of_snp} SNPs')
        plt.savefig(f'{summary_dir}tree_height/height_{mac_maf}_{num_of_snp}.png')

