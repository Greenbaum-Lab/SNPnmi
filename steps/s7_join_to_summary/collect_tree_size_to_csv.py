# python3 utils/scripts/collect_tree_size_to_csv.py -d hgdp
import json
import os
from os.path import dirname, abspath
import sys
import pandas as pd

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.common import get_paths_helper, args_parser, get_window_size, load_dict_from_json, class_iter
from utils.loader import Timer


def collect_tree_sizes_per_class(paths_helper, class_name, window_size, df):
    hash_json = paths_helper.hash_windows_list_template.format(class_name=class_name)
    raw_dict = load_dict_from_json(hash_json)
    length_dict = {k: int(len(v) * window_size) for (k, v) in raw_dict.items()}
    length_dict_path = paths_helper.hash_winds_lengths_template.format(class_name=class_name)
    with open(length_dict_path, "w") as f:
        json.dump(length_dict, f)
    if length_dict:
        for non_exists_hash in set(df.columns) - length_dict.keys():
            length_dict[non_exists_hash] = 0
        new_df = pd.DataFrame.from_records([length_dict], index=[class_name])
        df = df.append(new_df, sort=False)
    return df


def collect_tree_sizes_to_csv(options):
    paths_helper = get_paths_helper(options.dataset_name)
    window_size = get_window_size(paths_helper)
    df = pd.DataFrame()
    for cls in class_iter(options):
        df = collect_tree_sizes_per_class(paths_helper, cls.name, window_size, df)
    df.to_csv(paths_helper.tree_sizes, index_label='Class')


def main(options):
    with Timer(f"Collect tree sizes to csv"):
        collect_tree_sizes_to_csv(options)


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
