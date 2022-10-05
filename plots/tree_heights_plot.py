#!/usr/bin/env python
import os
import sys
from os.path import dirname, abspath
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from steps.s7_join_to_summary.collect_num_of_nodes import count_num_of_nodes_for_tree
from steps.s7_join_to_summary.collect_tree_heights import analyze_tree_heights
from steps.s7_join_to_summary.plots_helper import plot_per_class
from utils.common import get_paths_helper, class_iter

SCORE2COLOR_DICT = {'max_height': 'tab:blue', 'avg_height': 'tab:red', 'avg_leaves': 'tab:green',
                    'num_of_leaves': 'tab:blue', 'num_of_nodes': 'tab:red'}


def get_base_line_score(options):
    paths_helper = get_paths_helper(options.dataset_name)
    gt_dir = f'{paths_helper.net_struct_dir_class.format(class_name="all")}W_1_D_0_Min_{options.min_pop_size}_SS_{options.ns_ss}_B_1.0/'
    comm_analysis_file = [f for f in os.listdir(gt_dir) if "CommAnalysis" in f][0]
    structure_file = gt_dir + comm_analysis_file
    with open(structure_file, 'r') as f:
        tree_structure = f.readlines()
    max_height, avg_height, avg_leaves = analyze_tree_heights(tree_structure)
    num_of_nodes = count_num_of_nodes_for_tree(gt_dir + 'AllNodes.txt', options.min_pop_size)
    num_of_leaves = count_num_of_nodes_for_tree(gt_dir + '2_Leafs_NoOverlap.txt', options.min_pop_size)
    base_line = {'max_height': max_height,
                 'avg_height': avg_height,
                 'avg_leaves': avg_leaves,
                 'num_of_nodes': num_of_nodes,
                 'num_of_leaves': num_of_leaves}
    return base_line


def csv_to_plot(options, gt_scores, csv_path, plot_path, plot_title, scores):
    for num_of_snp in options.data_size:
        f = plt.figure()
        f.set_figwidth(8)
        f.set_figheight(6)
        path = csv_path.format(size=num_of_snp)
        df = pd.read_csv(path)
        for mac_maf in ['mac', 'maf']:
            plot_colors = []
            base_lines = []
            base_line_colors = []
            avg_arr = np.empty(shape=(0,))
            std_arr = np.empty(shape=(0,))
            for score in scores:
                avg = []
                std = []
                for cls in class_iter(options):
                    if cls.mac_maf != mac_maf:
                        continue
                    class_values = df[df.Class == cls.name]
                    avg.append(float(class_values[f'avg_{score}']))
                    std.append(float(class_values[f'std_{score}']))
                avg_arr = np.concatenate([avg_arr, np.array(avg)])
                std_arr = np.concatenate([std_arr, np.array(std)])
                plot_colors.append(SCORE2COLOR_DICT[score])
                base_line_colors.append(SCORE2COLOR_DICT[score])
                base_lines.append(gt_scores[score])
            avg_arr = avg_arr.reshape(len(scores), -1)
            std_arr = std_arr.reshape(len(scores), -1)

            plot_per_class(options, mac_maf,
                           values=avg_arr, std=std_arr,
                           scats=None, polynomials=None,
                           colors=plot_colors + base_line_colors,
                           base_lines=np.array(base_lines),
                           labels=scores,
                           title=f'{plot_title}',
                           legend_title="Scores",
                           output=plot_path.format(mac_maf=mac_maf, num_of_snp=num_of_snp))


def main(options):
    paths_helper = get_paths_helper(options.dataset_name)
    summary_dir = paths_helper.summary_dir
    os.makedirs(summary_dir + 'tree_height', exist_ok=True)
    gt_scores = get_base_line_score(options)

    scores = ['max_height', 'avg_height', 'avg_leaves']
    csv_to_plot(options, gt_scores, summary_dir + 'tree_heights_per_class_{size}.csv',
                summary_dir + 'tree_height/height_{mac_maf}_{num_of_snp}.svg',
                'PST depth properties',
                scores)
    scores = ['num_of_leaves', 'num_of_nodes']
    csv_to_plot(options, gt_scores, summary_dir + 'tree_num_of_nodes_per_class_{size}.csv',
                summary_dir + 'tree_height/nodes_{mac_maf}_{num_of_snp}.svg',
                'PST size properties',
                scores)
    return True
