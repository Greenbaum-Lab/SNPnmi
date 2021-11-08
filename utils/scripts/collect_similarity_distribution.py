# python3 utils/scripts/collect_tree_size_to_csv.py -d hgdp
import json
import os
from os.path import dirname, abspath, basename
import sys

import numpy as np
import pandas as pd
import re

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.common import get_paths_helper, args_parser
from utils.loader import Timer


def collect_similarity_distributions_per_class(paths_helper, class_name, df):
    similarity_dir = paths_helper.similarity_by_class_folder_template.format(class_name=class_name)
    files = [f for f in os.listdir() if "edges" in f and "all" not in f]
    for file in files:
        hash_tree = re.findall('[0-9]+', file)[-1]
        with open(file, "r") as f:
            edges = f.readlines()
        edges = np.array([float(e.split(" ")[2]) for e in edges])
        hist = np.histogram(edges, bins=np.linspace(0, 1, 100))


def collect_similarity_distributions(options):
    paths_helper = get_paths_helper(options.dataset_name)
    mac_min_range, mac_max_range = options.mac
    maf_min_range, maf_max_range = options.maf

    os.makedirs(paths_helper.summary_dir, exist_ok=True)
    csv_path = paths_helper.summary_dir + f'/distribution_similarity_per_tree_ss_{options.ns_ss}.csv'
    df = pd.DataFrame()

    for mac_maf in ['mac', 'maf']:
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        if min_range >= 0:
            for val in range(min_range, max_range + 1):
                # in maf we take 0.x
                if not is_mac:
                    val = f'{val * 1.0 / 100}'
                class_name = f'{mac_maf}_{val}'
                df = collect_similarity_distributions_per_class(paths_helper, class_name, df)
    df.to_csv(csv_path, index_label='Class')


def main(options):
    with Timer(f"Collect tree sizes to csv"):
        collect_similarity_distributions(options)


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
