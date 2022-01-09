#!/usr/bin/python3
# python3 utils/scripts/collect_num_of_nodes.py -d hgdp --args 1000
import os
from os.path import dirname, abspath, basename
import sys

import numpy as np
import pandas as pd
import re

from tqdm import tqdm

from utils.scripts.collect_tree_heights import combine_attributes_per_class

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.common import get_paths_helper, args_parser, load_dict_from_json, is_class_valid
from utils.loader import Timer

MIN_INDIVIDUAL_PER_NODE = 5


def collect_num_of_nodes_per_class(options, paths_helper, class_name, df):
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
        all_nodes_file = f'{tree_dir}AllNodes.txt'
        with open(all_nodes_file, "r") as f:
            num_of_nodes = 0
            all_nodes = f.readlines()
            for node in all_nodes:
                if len(node.split(" ")) >= MIN_INDIVIDUAL_PER_NODE:
                    num_of_nodes += 1
        all_leaves_files = f'{tree_dir}2_Leafs_NoOverlap.txt'
        with open(all_leaves_files, "r") as f:
            num_of_leaves = 0
            all_leaves = f.readlines()
            for leaf in all_leaves:
                if len(leaf.split(" ")) >= MIN_INDIVIDUAL_PER_NODE:
                    num_of_leaves += 1

        df_tree = pd.DataFrame([[tree_name, num_of_leaves, num_of_nodes]],
                               columns=["Tree", "num_of_leaves", "num_of_nodes"])
        df_class = df_class.append(df_tree, sort=False)
    df = df.append(df_class)
    return df


def collect_num_of_nodes(options):
    print("Stage 1")
    paths_helper = get_paths_helper(options.dataset_name)
    mac_min_range, mac_max_range = options.mac
    maf_min_range, maf_max_range = options.maf
    data_size = options.args[0]

    os.makedirs(paths_helper.summary_dir, exist_ok=True)
    csv_path = paths_helper.summary_dir + f'/num_of_nodes_{data_size}_ss_{options.ns_ss}.csv'
    df = pd.read_csv(csv_path) if os.path.exists(csv_path) else pd.DataFrame()

    for mac_maf in ['mac', 'maf']:
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        if min_range >= 0:
            for val in tqdm(range(min_range, max_range + 1), desc=f'Go over {mac_maf}'):
                if not is_class_valid(options, mac_maf, val):
                    continue
                # in maf we take 0.x
                if not is_mac:
                    val = f'{val * 1.0 / 100}'
                class_name = f'{mac_maf}_{val}'
                df = collect_num_of_nodes_per_class(options, paths_helper, class_name, df)
    df.to_csv(csv_path, index=False)
    return df


def combine_num_of_nodes_to_matrix(options, full_mat_df):
    print("stage 2")
    paths_helper = get_paths_helper(options.dataset_name)
    mac_min_range, mac_max_range = options.mac
    maf_min_range, maf_max_range = options.maf

    csv_output_path = paths_helper.summary_dir + f'/tree_num_of_nodes_per_class_{options.args[0]}.csv'
    sum_mat_df = pd.DataFrame(columns=['Class', 'avg_leaves', 'std_leaves', 'avg_nodes', 'std_nodes'])
    for mac_maf in ['mac', 'maf']:
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        for val in tqdm(range(min_range, max_range + 1), desc=f'Go over {mac_maf}'):
            if not is_class_valid(options, mac_maf, val):
                continue
            # in maf we take 0.x
            if not is_mac:
                val = f'{val * 1.0 / 100}'
            class_name = f'{mac_maf}_{val}'
            sum_mat_df = combine_attributes_per_class(class_name,
                                                      full_mat_df[full_mat_df['Tree'].str.contains(f'{class_name}_')],
                                                      sum_mat_df)
    sum_mat_df.to_csv(csv_output_path, index=False)


def main(options):
    with Timer(f"Collect num of nodes per tree to csv"):
        df = collect_num_of_nodes(options)
        combine_num_of_nodes_to_matrix(options, df)


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
