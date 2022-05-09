#!/usr/bin/python3

import itertools
import os
from os.path import dirname, abspath
import sys

import numpy as np
import pandas as pd
from tqdm import tqdm

from steps.s6_compare_to_random_pst.submit_run_nmi import get_gt_path_dictionary

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from steps.s7_join_to_summary import create_statistics_nmi_matrix
from steps.s7_join_to_summary.collect_tree_size_to_csv import collect_tree_sizes_to_csv
from utils.common import get_paths_helper, args_parser, class_iter
from utils.loader import Timer, Loader

NMI_TYPES = ['AllNodes', 'Leaves_WithOverlap']
SCORES = ['max', 'lfk', 'sum']
pairs = itertools.product(NMI_TYPES, SCORES)
ALL_SCORES_TYPES = [f'{p[0]}_{p[1]}' for p in pairs]


def get_scores_from_nmi_file(nmi_file):
    with open(nmi_file) as f:
        lines = f.readlines()
        max_score = float(lines[0].split('\t')[1])
        lfk_score = float(lines[2].split('\t')[1])
        sum_score = float(lines[3].split('\t')[1])
    return max_score, lfk_score, sum_score


def collect_nmi_per_class(options, paths_helper, class_name, df, tree_sizes, gt_name):
    df_class = pd.DataFrame()
    trees_in_df = list(df['Tree']) if 'Tree' in df.columns else []
    for hash_idx in tree_sizes:
        tree_name = f'{class_name}_{hash_idx}'
        if tree_name in trees_in_df:
            continue
        df_tree = pd.DataFrame(columns=["Tree", "Size"] + ALL_SCORES_TYPES)
        if np.all(np.isnan(tree_sizes[hash_idx])):
            continue
        df_tree['Tree'] = [tree_name]
        df_tree['Size'] = int(tree_sizes[hash_idx])
        is_tree_valid = int(df_tree['Size']) > 0
        for nmi_type in NMI_TYPES:
            nmi_type_file = paths_helper.nmi_file_template.format(gt_name=gt_name, class_name=class_name,
                                                                  tree_hash=hash_idx, ns_ss=options.ns_ss,
                                                                  nmi_type=nmi_type)
            if not os.path.exists(nmi_type_file):
                is_tree_valid = False
                continue
            scores = get_scores_from_nmi_file(nmi_type_file)
            for i in range(len(SCORES)):
                df_tree[[f'{nmi_type}_{SCORES[i]}']] = scores[i]
        if is_tree_valid:
            df_class = df_class.append(df_tree, sort=False)
    df = df.append(df_class, sort=False)
    return df


def collect_nmi(options):
    paths_helper = get_paths_helper(options.dataset_name)
    gt_paths = get_gt_path_dictionary(options, paths_helper)
    for gt_name, gt_path in gt_paths.items():
        csv_path = paths_helper.summary_dir + f'/nmi_matrix_{gt_name}_ss_{options.ns_ss}.csv'
        t_size = pd.read_csv(paths_helper.tree_sizes)

        df = pd.read_csv(csv_path) if os.path.exists(csv_path) else pd.DataFrame()

        for cls in tqdm(list(class_iter(options)), desc='collect nmi'):
            df = collect_nmi_per_class(options, paths_helper, cls.name, df,
                                       t_size[t_size['Class'] == cls.name].drop(['Class'], axis=1))
        df.to_csv(csv_path, index=False)


def main(options):
    with Timer(f"Collect nmi"):
        collect_tree_sizes_to_csv(options)
        collect_nmi(options)
        create_statistics_nmi_matrix.main(options)
    return True

if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
