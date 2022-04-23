#!/usr/bin/python3
# utils/scripts/collect_num_of_nodes.py -d hgdp --args 1000

import os
from os.path import dirname, abspath
import sys
import pandas as pd
from tqdm import tqdm

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from steps.s7_join_to_summary.collect_tree_heights import combine_attributes_per_class
from utils.common import get_paths_helper, args_parser, load_dict_from_json, class_iter
from utils.loader import Timer

MIN_INDIVIDUAL_PER_NODE = 5


def collect_num_of_nodes_per_class(options, paths_helper, class_name, df, data_size):
    trees_in_df = list(df['Tree']) if 'Tree' in df.columns else []
    df_class = pd.DataFrame()
    tree_nums = load_dict_from_json(paths_helper.hash_winds_lengths_template.format(class_name=class_name))
    tree_dirs = {h: paths_helper.net_struct_dir_class.format(class_name=class_name, tree_hash=h) for h in
                 tree_nums.keys()}
    tree_length_dict = load_dict_from_json(paths_helper.hash_winds_lengths_template.format(class_name=class_name))
    for tree_hash, dir in tree_dirs.items():
        tree_name = f'{class_name}_{tree_hash}'
        if tree_name in trees_in_df:
            continue
        if tree_length_dict[tree_hash] != data_size:
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
                if len(node.split(" ")) - 1 >= MIN_INDIVIDUAL_PER_NODE:
                    num_of_nodes += 1
        all_leaves_files = f'{tree_dir}2_Leafs_NoOverlap.txt'
        with open(all_leaves_files, "r") as f:
            num_of_leaves = 0
            all_leaves = f.readlines()
            for leaf in all_leaves:
                if len(leaf.split(" ")) - 1 >= MIN_INDIVIDUAL_PER_NODE:
                    num_of_leaves += 1

        df_tree = pd.DataFrame([[tree_name, num_of_leaves, num_of_nodes]],
                               columns=["Tree", "num_of_leaves", "num_of_nodes"])
        df_class = df_class.append(df_tree, sort=False)
    df = df.append(df_class)
    return df


def collect_num_of_nodes(options,data_size):
    paths_helper = get_paths_helper(options.dataset_name)

    os.makedirs(paths_helper.summary_dir, exist_ok=True)
    csv_path = paths_helper.summary_dir + f'/num_of_nodes_{data_size}_ss_{options.ns_ss}.csv'
    df = pd.read_csv(csv_path) if os.path.exists(csv_path) else pd.DataFrame()

    for cls in tqdm(list(class_iter(options)), desc='collect num of nodes Stage 1'):
        df = collect_num_of_nodes_per_class(options, paths_helper, cls.name, df, data_size)
    df.to_csv(csv_path, index=False)
    return df


def combine_num_of_nodes_to_matrix(options, full_mat_df, data_size):
    paths_helper = get_paths_helper(options.dataset_name)

    csv_output_path = paths_helper.summary_dir + f'/tree_num_of_nodes_per_class_{data_size}.csv'
    sum_mat_df = pd.DataFrame(columns=['Class', 'avg_leaves', 'std_leaves', 'avg_nodes', 'std_nodes'])
    for cls in tqdm(list(class_iter(options)), desc='collect num of nodes Stage 2'):
        sum_mat_df = combine_attributes_per_class(cls.name,
                                                      full_mat_df[full_mat_df['Tree'].str.contains(f'{cls.name}_')],
                                                      sum_mat_df)
    sum_mat_df.to_csv(csv_output_path, index=False)


def main(options):
    with Timer(f"Collect num of nodes per tree to csv"):
        for data_size in [1000, 5000]:
            df = collect_num_of_nodes(options, data_size)
            combine_num_of_nodes_to_matrix(options, df, data_size)
    return True

if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
