#!/usr/bin/python3
# python3 utils/scripts/collect_tree_heights.py -d hgdp --args 1000
import os
from os.path import dirname, abspath, basename
import sys

import numpy as np
import pandas as pd
import re

from tqdm import tqdm

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.common import get_paths_helper, args_parser, load_dict_from_json, class_iter
from utils.loader import Timer


def analyze_tree_heights(tree_struct):
    level_indices = [i for i in range(len(tree_struct)) if '--- LEVEL' in tree_struct[i]]
    max_level = len(level_indices) - 1
    nodes_per_level = np.diff(level_indices + [len(tree_struct)]) - 1
    num_of_nodes = np.sum(nodes_per_level)
    avg_height = np.sum(nodes_per_level * np.arange(max_level + 1)) / num_of_nodes
    leaves = []
    for node in np.delete(tree_struct, level_indices):
        match = re.search('Level_(\d+)_Entry_(\d+)_Line_(\d+)', node)
        level = match.group(1)
        entry = match.group(2)
        line = match.group(3)
        is_leaf = True
        for other in tree_struct:
            if f'ParentLevel_{level}_ParentEntry_{entry}_ParentLine_{line}_' in other:
                is_leaf = False
        if is_leaf:
            leaves.append(int(level))
    avg_leaves = np.mean(leaves)
    return max_level, avg_height, avg_leaves


def collect_tree_heights_per_class(options, paths_helper, class_name, df):
    trees_in_df = list(df['Tree']) if 'Tree' in df.columns else []
    df_class = pd.DataFrame()
    tree_nums = load_dict_from_json(paths_helper.hash_winds_lengths_template.format(class_name=class_name))
    tree_dirs = {h: paths_helper.net_struct_dir_class.format(class_name=class_name, tree_hash=h) for h in
                 tree_nums.keys()}
    tree_length_dict = load_dict_from_json(paths_helper.hash_winds_lengths_template.format(class_name=class_name))
    tree_size = options.args[0]
    for tree_hash, dir in tree_dirs.items():
        tree_name = f'{class_name}_{tree_hash}'
        if tree_name in trees_in_df:
            continue
        if tree_length_dict[tree_hash] != tree_size:
            continue
        sub_trees = os.listdir(f'{dir}{tree_name}/')
        correct_trees = [t for t in sub_trees if f"SS_{options.ns_ss}" in t]
        assert len(correct_trees) == 1, f"more than 1 tree or tree {tree_hash} for class {class_name} is missing"
        tree_dir = f'{dir}{tree_name}/{correct_trees[0]}/'
        comm_analysis_file = [f for f in os.listdir(tree_dir) if "CommAnalysis" in f][0]
        with open(tree_dir + comm_analysis_file, "r") as f:
            tree_structure = f.readlines()
        max_height, avg_height, avg_leaves = analyze_tree_heights(tree_structure)

        df_tree = pd.DataFrame([[tree_name, max_height, avg_height, avg_leaves]],
                               columns=["Tree", "max_height", "avg_height", "avg_leaves"])
        df_class = df_class.append(df_tree, sort=False)
    df = df.append(df_class)
    return df


def collect_tree_heights(options, size):
    print(f"collect tree heights for size {size}")
    paths_helper = get_paths_helper(options.dataset_name)
    data_size = options.args[0]

    os.makedirs(paths_helper.summary_dir, exist_ok=True)
    csv_path = paths_helper.summary_dir + f'/tree_heights_{data_size}_ss_{options.ns_ss}.csv'
    df = pd.read_csv(csv_path) if os.path.exists(csv_path) else pd.DataFrame()

    for cls in tqdm(list(class_iter(options)), desc='collect tree height Stage 1'):
        df = collect_tree_heights_per_class(options, paths_helper, cls.name, df)
    df.to_csv(csv_path, index=False)
    return df


def combine_attributes_per_class(class_name, input_df, sum_df):
    class_df = pd.DataFrame(columns=sum_df.columns)
    class_df['Class'] = [class_name]
    for c in input_df.columns:
        if c == 'Tree':
            continue
        avg = np.mean(input_df[c])
        class_df[f'avg_{c}'] = [avg]
        std = np.std(input_df[c])
        class_df[f'std_{c}'] = [std]
    sum_df = sum_df.append(class_df, sort=False)
    return sum_df


def combine_heights_to_sum_matrix(options, full_mat_df, size):
    print(f"combine heights to sum matrix for size {size}")
    paths_helper = get_paths_helper(options.dataset_name)
    csv_output_path = paths_helper.summary_dir + f'/tree_heights_per_class_{size}.csv'
    sum_mat_df = pd.DataFrame(columns=['Class', 'avg_max_height', 'std_max_height', 'avg_avg_height', 'std_avg_height',
                                       'avg_avg_leaves', 'std_avg_leaves'])
    for cls in tqdm(list(class_iter(options)), desc='collect tree height Stage 2'):
        sum_mat_df = combine_attributes_per_class(cls.name,
                                                  full_mat_df[full_mat_df['Tree'].str.contains(f'{cls.name}_')],
                                                  sum_mat_df)
    sum_mat_df.to_csv(csv_output_path, index=False)


def main(options):
    with Timer(f"Collect tree heights to csv"):
        for size in ['1000', '5000']:
            df = collect_tree_heights(options, size)
            combine_heights_to_sum_matrix(options, df, size)


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
