#!/usr/bin/env python
import sys
from os.path import dirname, abspath
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from steps.s7_join_to_summary.plots_helper import PlotConsts

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.common import get_paths_helper, class_iter

SCORE2COLOR_DICT = {'max_height': 'b', 'avg_height': 'r', 'avg_leaves': 'g', 'num_of_leaves': 'b', 'num_of_nodes': 'r'}
gt_nodes = {'hgdp': (77, 36), 'arabidopsis': (194, 62)}

def csv_to_plot(options, csv_path, plot_path, title, scores):
    for num_of_snp in [1000, 5000]:
        f = plt.figure()
        f.set_figwidth(8)
        f.set_figheight(6)
        path = csv_path.format(size=num_of_snp)
        df = pd.read_csv(path)
        for mac_maf in ['mac', 'maf']:
            class_names = PlotConsts.get_class_names(options, mac_maf)
            for score in scores:
                avg = []
                std = []
                for cls in class_iter(options):
                    if cls.mac_maf != mac_maf:
                        continue
                    class_values = df[df.Class == cls.name]
                    avg.append(float(class_values[f'avg_{score}']))
                    std.append(float(class_values[f'std_{score}']))
                avg = np.array(avg)
                std = np.array(std)
                plt.plot(class_names, avg, color=SCORE2COLOR_DICT[score], label=score,
                         linewidth=PlotConsts.line_width)
                plt.fill_between(class_names, y1=avg - std, y2=avg + std, alpha=0.3)
                if score == 'num_of_leaves':
                    plt.plot([min(class_names), max(class_names)],
                             [gt_nodes[options.dataset_name][1] for _ in range(2)],
                             linestyle='--', color=SCORE2COLOR_DICT['num_of_leaves'], alpha=0.8,
                             linewidth=PlotConsts.line_width)
                if score == 'num_of_nodes':
                    plt.plot([min(class_names), max(class_names)],
                             [gt_nodes[options.dataset_name][0] for _ in range(2)],
                             linestyle='--', color=SCORE2COLOR_DICT['num_of_nodes'], alpha=0.8,
                             linewidth=PlotConsts.line_width)
            plt.xlabel(f"{mac_maf}", fontsize=16)
            legend = plt.legend(title="Scores", loc='upper left', fontsize=12)
            plt.setp(legend.get_title(), fontsize=12)
            plt.xticks(fontsize=10)
            plt.yticks(fontsize=10)
            plt.title(title, fontsize=18)
            plt.savefig(plot_path.format(mac_maf=mac_maf, num_of_snp=num_of_snp))
            plt.clf()

def main(options):
    paths_helper = get_paths_helper(options.dataset_name)
    summary_dir = paths_helper.summary_dir
    scores = ['max_height', 'avg_height', 'avg_leaves']
    csv_to_plot(options, summary_dir + 'tree_heights_per_class_{size}.csv',
                summary_dir + 'tree_height/height_{mac_maf}_{num_of_snp}.svg',
                'PST depth properties',
                scores)
    scores = ['num_of_leaves', 'num_of_nodes']
    csv_to_plot(options, summary_dir + 'tree_num_of_nodes_per_class_{size}.csv',
                summary_dir + 'tree_height/nodes_{mac_maf}_{num_of_snp}.svg',
                'PST size properties',
                scores)
