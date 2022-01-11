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

mac_min_range, mac_max_range = options.mac
maf_min_range, maf_max_range = options.maf
SCORE2COLOR_DICT = {'max_height': 'b', 'avg_height': 'r', 'avg_leaves': 'g', 'num_of_leaves': 'b', 'num_of_nodes': 'r'}
gt_nodes = {'hgdp': (77, 36), 'arabidopsis': (194, 62)}
mac_class_names = np.arange(mac_min_range, mac_max_range + 1) if options.dataset_name != 'arabidopsis' else np.arange(
    mac_min_range, mac_max_range + 1, 2)
maf_class_names = np.arange(maf_min_range, maf_max_range + 1) / 100


def csv_to_plot(csv_path, plot_path):
    for num_of_snp in [1000, 5000]:
        f = plt.figure()
        f.set_figwidth(15)
        f.set_figheight(15)
        path = csv_path.format(size=num_of_snp)
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
                    for val in range(min_range, max_range + 1):
                        if not is_class_valid(options, mac_maf, val):
                            continue
                        # in maf we take 0.x
                        float_val = val if is_mac else val * 1.0 / 100
                        class_name = f"{mac_maf}_{float_val}"
                        class_values = df[df.Class == class_name]
                        avg.append(float(class_values[f'avg_{score}']))
                        std.append(float(class_values[f'std_{score}']))
                avg = np.array(avg)
                std = np.array(std)
                plt.plot(class_names, avg, color=SCORE2COLOR_DICT[score], label=score)
                plt.fill_between(class_names, y1=avg - std, y2=avg + std, alpha=0.3)
                if score == 'num_of_leaves':
                    plt.plot([gt_nodes[options.dataset_name][1] for _ in range(2)], [min(class_names), max(class_names)],
                             linestyle='--', color=SCORE2COLOR_DICT['num_of_leaves'], alpha=0.5)
                if score == 'num_of_nodes':
                    plt.plot([gt_nodes[options.dataset_name][0] for _ in range(2)],
                             [min(class_names), max(class_names)],
                             linestyle='--', color=SCORE2COLOR_DICT['num_of_nodes'], alpha=0.5)
            plt.xlabel(f"{mac_maf}", fontsize=20)
            legend = plt.legend(title="Scores", loc='upper left', fontsize=20)
            plt.setp(legend.get_title(), fontsize=20)
            plt.title(f'Structure depth with {num_of_snp} SNPs', fontsize=20)
            plt.savefig(plot_path.format(mac_maf=mac_maf, num_of_snp=num_of_snp))
            plt.clf()


if options.args[0] == 1:
    SCORES = ['max_height', 'avg_height', 'avg_leaves']
    csv_to_plot(summary_dir + 'tree_heights_per_class_{size}.csv',
                summary_dir + 'tree_height/height_{mac_maf}_{num_of_snp}.svg')
elif options.args[0] == 2:
    SCORES = ['max_height', 'avg_height', 'avg_leaves']
    csv_to_plot(summary_dir + 'tree_heights_per_class_{size}.csv',
                summary_dir + 'tree_height/height_{mac_maf}_{num_of_snp}.svg')
    SCORES = ['num_of_leaves', 'num_of_nodes']
    csv_to_plot(summary_dir + 'tree_num_of_nodes_per_class_{size}.csv',
                summary_dir + 'tree_height/nodes_{mac_maf}_{num_of_snp}.svg')
elif options.args[0] == 3:
    SCORES = ['num_of_leaves', 'num_of_nodes']
    csv_to_plot(summary_dir + 'tree_num_of_nodes_per_class_{size}.csv',
                summary_dir + 'tree_height/nodes_{mac_maf}_{num_of_snp}.svg')
else:
    raise ValueError
