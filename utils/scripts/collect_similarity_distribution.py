# python3 utils/scripts/collect_similarity_distribution.py -d hgdp --args 1000
import os
from os.path import dirname, abspath, basename
import sys

import numpy as np
import pandas as pd
import re

from tqdm import tqdm

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.common import get_paths_helper, args_parser, load_dict_from_json
from utils.loader import Timer


def collect_similarity_distributions_per_class(options, paths_helper, class_name, bins, df):
    similarity_dir = paths_helper.similarity_by_class_folder_template.format(class_name=class_name)
    trees_in_df = list(df['Tree']) if 'Tree' in df.columns else []
    df_class = pd.DataFrame()
    files = [f for f in os.listdir(similarity_dir) if "edges" in f and "all" not in f]
    hash_length_path = paths_helper.hash_winds_lengths_template.format(class_name=class_name)
    tree_length_dict = load_dict_from_json(hash_length_path)
    tree_size = options.args[0]
    for file in tqdm(files, leave=False):
        hash_tree = re.findall('[0-9]+', file)[-1]
        tree_name = f'{class_name}_{hash_tree}'
        if tree_name in trees_in_df:
            continue
        assert str(
            hash_tree) in tree_length_dict.keys(), f"class {class_name} tree {tree_name} is missing from tree size file!"
        if tree_length_dict[hash_tree] != tree_size:
            continue
        with open(similarity_dir + file, "r") as f:
            edges = f.readlines()
        edges = np.array([float(e.split(" ")[2]) for e in edges])
        hist = np.histogram(edges, bins=np.linspace(0, 1, bins))[0]
        df_tree = pd.DataFrame([[tree_name, edges.mean(), np.median(edges)] + list(hist)],
                               columns=["Tree", "mean", "median"] + [str(round(e, 10)) for e in np.linspace(0, 1, bins)][:-1])
        df_class = df_class.append(df_tree, sort=False)
    df = df.append(df_class)
    return df


def collect_similarity_distributions(options):
    print("Stage 1")
    paths_helper = get_paths_helper(options.dataset_name)
    mac_min_range, mac_max_range = options.mac
    maf_min_range, maf_max_range = options.maf
    tree_size = options.args[0]

    os.makedirs(paths_helper.summary_dir, exist_ok=True)
    csv_path = paths_helper.summary_dir + f'/distribution_similarity_per_tree_{tree_size}.csv'
    df = pd.read_csv(csv_path) if os.path.exists(csv_path) else pd.DataFrame()
    bins = int(1 / float(options.ns_ss) + 1)
    for mac_maf in ['mac', 'maf']:
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        if min_range >= 0:
            for val in tqdm(range(min_range, max_range + 1), desc=f'Go over {mac_maf}'):
                # in maf we take 0.x
                if not is_mac:
                    val = f'{val * 1.0 / 100}'
                class_name = f'{mac_maf}_{val}'
                df = collect_similarity_distributions_per_class(options, paths_helper, class_name, bins, df)
            df.to_csv(csv_path, index=False)
    return df


def combine_distributions_per_class(class_name, input_df, sum_df):
    class_df = pd.DataFrame(columns=sum_df.columns)
    class_df['Class'] = [class_name]
    for c in input_df.columns:
        if c == 'Tree':
            continue
        avg = np.mean(input_df[c])
        class_df[f'avg_{c}'] = [avg]
        std = np.std(input_df[c])
        class_df[f'std_{c}'] = [std]
    sum_df = sum_df.append(class_df)
    return sum_df


def combine_distributions_to_sum_matrix(options, full_mat_df):
    print("stage 2")
    paths_helper = get_paths_helper(options.dataset_name)
    mac_min_range, mac_max_range = options.mac
    maf_min_range, maf_max_range = options.maf
    bins = int(1 / float(options.ns_ss) + 1)

    csv_output_path = paths_helper.summary_dir + f'/distribution_similarity_per_class_{options.args[0]}.csv'
    sum_mat_df = pd.DataFrame(columns=['Class', 'avg_mean', 'std_mean', 'avg_median', 'std_median'] +
                                      [f'avg_{round(e,10)}' for e in np.linspace(0, 1, bins)] +
                                      [f'std_{round(e,10)}' for e in np.linspace(0, 1, bins)])
    for mac_maf in ['mac', 'maf']:
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        for val in tqdm(range(min_range, max_range + 1), desc=f'Go over {mac_maf}'):
            # in maf we take 0.x
            if not is_mac:
                val = f'{val * 1.0 / 100}'
            class_name = f'{mac_maf}_{val}'
            sum_mat_df = combine_distributions_per_class(class_name, full_mat_df[
                full_mat_df['Tree'].str.contains(f'{class_name}_')], sum_mat_df)
    sum_mat_df.to_csv(csv_output_path, index=False)


def main(options):
    with Timer(f"Collect similarity distribution to csv"):
        df = collect_similarity_distributions(options)
        combine_distributions_to_sum_matrix(options, df)


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
