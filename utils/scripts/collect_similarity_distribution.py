# python3 utils/scripts/collect_similarity_distribution.py -d hgdp
import os
from os.path import dirname, abspath, basename
import sys

import numpy as np
import pandas as pd
import re

from tqdm import tqdm

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.common import get_paths_helper, args_parser
from utils.loader import Timer


def collect_similarity_distributions_per_class(options, paths_helper, class_name, df):
    similarity_dir = paths_helper.similarity_by_class_folder_template.format(class_name=class_name)
    trees_in_df = list(df['Tree']) if 'Tree' in df.columns else []
    df_class = {}
    files = [f for f in os.listdir(similarity_dir) if "edges" in f and "all" not in f]
    bins = int(1 / float(options.ns_ss) + 1)
    i = 0
    for file in files:
        hash_tree = re.findall('[0-9]+', file)[-1]
        tree_name = f'{class_name}_{hash_tree}'
        if tree_name in trees_in_df:
            continue
        i += 1
        with open(similarity_dir + file, "r") as f:
            edges = f.readlines()
        edges = np.array([float(e.split(" ")[2]) for e in edges])
        hist = np.histogram(edges, bins=np.linspace(0, 1, bins))[0]
        df_class[i] = {"Tree": tree_name, "mean": edges.mean(), "median": np.median(edges)}.update(
            {k: val for (k, val) in zip(list(hist), [str(e) for e in np.linspace(0, 1, bins)][:-1])}
        )
    df_class = pd.DataFrame.from_dict(df_class)
    df = df.append(df_class, sort=False)
    return df


def collect_similarity_distributions(options):
    paths_helper = get_paths_helper(options.dataset_name)
    mac_min_range, mac_max_range = options.mac
    maf_min_range, maf_max_range = options.maf

    os.makedirs(paths_helper.summary_dir, exist_ok=True)
    csv_path = paths_helper.summary_dir + f'/distribution_similarity_per_tree_ss_{options.ns_ss}.csv'
    df = pd.read_csv(csv_path) if os.path.exists(csv_path) else pd.DataFrame()

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
                df = collect_similarity_distributions_per_class(options, paths_helper, class_name, df)
                print(f"Done with {class_name}")
    df.to_csv(paths_helper.summary_dir + f'/distribution_similarity_per_tree_ss_{options.ns_ss}2.csv', index=False)


def main(options):
    with Timer(f"Collect similarity distribution to csv"):
        collect_similarity_distributions(options)


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
