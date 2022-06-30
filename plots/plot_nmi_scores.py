#!/usr/bin/python3
import os
import sys
from os.path import dirname, abspath, basename

from steps.s6_compare_to_random_pst.submit_run_nmi import get_gt_path_dictionary

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from steps.s7_join_to_summary.plots_helper import plot_per_class, PlotConsts
from utils.loader import Timer
from utils.checkpoint_helper import execute_with_checkpoint
from utils.common import get_paths_helper, class_iter, str_for_timer
import matplotlib.pyplot as plt
import pandas as pd
import itertools
import numpy as np


SCRIPT_NAME = basename(__file__)


def _get_scores_from_nmi_file(nmi_file):
    with open(nmi_file) as f:
        lines = f.readlines()
        max_score = float(lines[0].split('\t')[1])
        lfkScore = float(lines[2].split('\t')[1])
        sum_score = float(lines[3].split('\t')[1])
        return max_score, lfkScore, sum_score


def get_inputs_for_plot_func(options, gt_name):
    paths_helper = get_paths_helper(options.dataset_name)
    summary_dir = paths_helper.summary_dir
    nmi_dir = paths_helper.nmi_dir.format(gt_name=gt_name)
    nmi_matrix_path = summary_dir + f'nmi_{gt_name}_sum_matrix.csv'
    nmi_file_template = '{mac_maf}_{val}/{mac_maf}_{val}_all/step_{ns_ss}/{input_type}.txt'

    df = pd.read_csv(nmi_matrix_path)
    NMI_TYPES = ['AllNodes', 'Leaves_WithOverlap']
    SCORES = ['max', 'lfk']
    nmi_type_score_pairs = list(itertools.product(NMI_TYPES, SCORES))
    return nmi_type_score_pairs, df, nmi_dir, nmi_file_template, summary_dir


def plot_nmi_scores(options):
    paths_helper = get_paths_helper(options.dataset_name)
    gt_paths = get_gt_path_dictionary(options, paths_helper)
    for gt_name, gt_path in gt_paths.items():
        pairs, df, nmi_dir, nmi_file_template, summary_dir = get_inputs_for_plot_func(options, gt_name)
        os.makedirs(f'{summary_dir}nmi_{gt_name}_scores', exist_ok=True)
        for nmi_type, score in pairs:
            score_name = f'{nmi_type}_{score}'
            nmi_type_rep = 'Full PST' if nmi_type == 'AllNodes' else 'Fine Scale'

            for mac_maf in ['mac', 'maf']:
                all_classes_avg = []
                class_names = PlotConsts.get_class_names(options, mac_maf)
                f = plt.figure()
                f.set_figwidth(8)
                f.set_figheight(6)
                avg_arr = np.empty(shape=(0,))
                std_arr = np.empty(shape=(0,))
                for num_of_snp in options.data_size:
                    avg = []
                    std = []
                    for cls in class_iter(options):
                        if cls.mac_maf != mac_maf:
                            continue
                        if num_of_snp == options.data_size[0]:
                            all_class_file = nmi_dir + nmi_file_template.format(gt_name=gt_name, mac_maf=mac_maf, val=cls.val,
                                                                                ns_ss=options.ns_ss, input_type=nmi_type)
                            max_score, lfk_score, sum_score = _get_scores_from_nmi_file(all_class_file)
                            all_classes_avg.append(max_score)
                        class_name = f'ss_{options.ns_ss}_{mac_maf}_{cls.val}_{num_of_snp}'
                        class_values = df[df.Class == class_name]
                        if len(class_values) == 0:
                            continue
                        avg.append(float(class_values[f'{score_name}_avg']))
                        std.append(float(class_values[f'{score_name}_std']))
                    avg_arr = np.concatenate([avg_arr, np.array(avg)])
                    std_arr = np.concatenate([std, np.array(std)])
                avg_arr = avg_arr.reshape(len(options.data_size), -1)
                std_arr = std_arr.reshape(len(options.data_size), -1)
                polynomial = np.array([np.polyfit(class_names, all_classes_avg, 3)]).reshape(1, -1)
                plot_per_class(options, mac_maf,
                               values=avg_arr,
                               std=std_arr,
                               scats=np.array(all_classes_avg).reshape(1, -1),
                               colors=['tab:blue', 'tab:green', 'tab:orange'],
                               polynomials=polynomial,
                               base_lines=None,
                               labels=[f'{e} SNPs' for e in options.data_size] + ['Full class'],
                               title=f'Simulation - {nmi_type_rep} - {mac_maf}',
                               y_label="NMI score",
                               legend_title="Num of SNPs",
                               output=f'{summary_dir}nmi_{gt_name}_scores/{mac_maf}_{score_name}.svg')
    return True


def main(options):
    with Timer(f"plot nmi scores {str_for_timer(options)}"):
        is_success, msg = execute_with_checkpoint(plot_nmi_scores, SCRIPT_NAME, options)
    return is_success