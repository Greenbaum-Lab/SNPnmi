# python3 utils/scripts/collect_tree_size_to_csv.py -d hgdp
import os
from os.path import dirname, abspath, basename
import sys
import pandas as pd

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from steps.s5_build_baseline_pst.per_class_sum_n_windows import load_hash_data
from utils.common import get_paths_helper, args_parser, get_window_size
from utils.loader import Timer


def collect_tree_sizes_per_class(paths_helper, class_name, window_size, df):
    hash_json = paths_helper.hash_windows_list_template.format(class_name=class_name)
    raw_dict = load_hash_data(hash_json)
    length_dict = {k: len(v) * window_size for (k, v) in raw_dict.items()}
    if length_dict:
        new_df = pd.DataFrame.from_records([length_dict], index=[class_name])
        df = df.append(new_df)
    return df



def collect_tree_sizes_to_csv(options):
    paths_helper = get_paths_helper(options.dataset_name)
    mac_min_range, mac_max_range = options.mac
    maf_min_range, maf_max_range = options.maf

    window_size = get_window_size(paths_helper)
    os.makedirs(paths_helper.summary_dir, exist_ok=True)
    csv_path = paths_helper.summary_dir + '/tree_sizes.csv'
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
                df = collect_tree_sizes_per_class(paths_helper, class_name, window_size, df)
    df.to_csv(csv_path)


def main(options):
    with Timer(f"Collect tree sizes to csv"):
        collect_tree_sizes_to_csv(options)


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
