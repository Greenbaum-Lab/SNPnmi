# python3 utils/scripts/collect_nmi.py -d hgdp
import itertools
import os
from os.path import dirname, abspath, basename
import sys
import pandas as pd


root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.scripts.collect_tree_size_to_csv import collect_tree_sizes_to_csv
from utils.common import get_paths_helper, args_parser, get_window_size
from utils.loader import Timer

NMI_TYPES = ['AllNodes', 'Leaves_NoOverlap', 'Leaves_WithOverlap']
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


def collect_nmi_per_class(options, paths_helper, class_name, df, tree_sizes):
    for hash_idx in tree_sizes:
        tree_name = [f'{class_name}_{hash_idx}']
        df_tree = pd.DataFrame(columns=["Size"] + ALL_SCORES_TYPES, index=[f'{tree_name}'])
        df_tree['Size'] = int(tree_sizes[hash_idx])
        for nmi_type in NMI_TYPES:
            nmi_type_file = paths_helper.nmi_file_template.format(class_name=class_name, tree_hash=hash_idx,
                                                                  ns_ss=options.ns_ss, nmi_type=nmi_type)
            if not os.path.exists(nmi_type_file):
                continue
            scores = get_scores_from_nmi_file(nmi_type_file)
            for i in range(len(SCORES)):
                df_tree[[f'{nmi_type}_{SCORES[i]}']] = scores[i]
        df = df.append(df_tree)
    return df


def collect_nmi(options):
    paths_helper = get_paths_helper(options.dataset_name)
    mac_min_range, mac_max_range = options.mac
    maf_min_range, maf_max_range = options.maf

    window_size = get_window_size(paths_helper)
    os.makedirs(paths_helper.summary_dir, exist_ok=True)
    csv_path = paths_helper.summary_dir + f'/nmi_matrix_ss_{options.ns_ss}.csv'
    t_size = pd.read_csv(paths_helper.tree_sizes)

    df = pd.DataFrame()

        # go over classes
    for mac_maf in ['mac', 'maf']:
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        if min_range > 0:
            for val in range(min_range, max_range + 1):
                # in maf we take 0.x
                if not is_mac:
                    val = f'{val * 1.0 / 100}'
                class_name = f'{mac_maf}_{val}'
                df = collect_nmi_per_class(options, paths_helper, class_name, df,
                                           t_size[t_size['Class'] == class_name].drop(['Class'], axis=1))

    df.to_csv(csv_path, index_label='Class')



def main(options):
    with Timer(f"Collect tree sizes to csv"):
        collect_tree_sizes_to_csv(options)
        collect_nmi(options)


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
