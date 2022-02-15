# python3 utils/scripts/create_statistics_nmi_matrix.py -d hgdp

from os.path import dirname, abspath, basename
import sys

import numpy as np
import pandas as pd
from tqdm import tqdm

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.scripts.collect_nmi import ALL_SCORES_TYPES
from utils.common import get_paths_helper, args_parser, class_iter
from utils.loader import Timer


def summarize_nmi_per_class(ns_ss, class_name, df, one_class_df):
    sizes = {}
    for tree_tuple in one_class_df.iterrows():
        tree = tree_tuple[1]
        size = tree['Size']
        if size not in sizes:
            sizes[size] = {score: [] for score in ALL_SCORES_TYPES}
            sizes[size].update({'num_of_trees': 0})
        sizes[size]['num_of_trees'] += 1
        for score in ALL_SCORES_TYPES:
            sizes[size][score].append(tree[score])
    for size, size_dict in sizes.items():
        df_class_size = pd.DataFrame(columns=["num_of_trees"] + [f'{t}_avg' for t in ALL_SCORES_TYPES] +
                                             [f'{t}_std' for t in ALL_SCORES_TYPES], index=[f'ss_{ns_ss}_{class_name}_{size}'])
        df_class_size['num_of_trees'] = size_dict['num_of_trees']
        for score in ALL_SCORES_TYPES:
            df_class_size[f'{score}_avg'] = np.mean(size_dict[score])
            df_class_size[f'{score}_std'] = np.std(size_dict[score])
        df = df.append(df_class_size, sort=False)
    return df


def summarize_nmi_mat(options, paths_helper, ns_ss, df):
    nmi_full_matrix_path = paths_helper.summary_dir + f'nmi_matrix_ss_{ns_ss}.csv'
    nmi_matrix = pd.read_csv(nmi_full_matrix_path)

    for cls in tqdm(list(class_iter(options))):
        df = summarize_nmi_per_class(ns_ss, cls.name, df, nmi_matrix[nmi_matrix['Tree'].str.contains(f'{cls.name}_')])
    return df


def main(options):
    with Timer(f"Create statistics matrix"):
        paths_helper = get_paths_helper(options.dataset_name)
        ns_ss_args = options.ns_ss.split(',')
        ns_ss_args = [float(i) for i in ns_ss_args]
        df = pd.DataFrame()
        output_matrix_path = paths_helper.summary_dir + f'nmi_sum_matrix.csv'

        for ss in ns_ss_args:
            df = summarize_nmi_mat(options, paths_helper, ss, df)
        df.to_csv(output_matrix_path, index_label='Class')

if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
