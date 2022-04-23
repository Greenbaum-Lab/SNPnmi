#!/usr/bin/env python
import os
import sys
from os.path import dirname, abspath, basename
from tqdm import tqdm
import numpy as np


root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from steps.s7_join_to_summary.plots_helper import plot_per_class
from utils.checkpoint_helper import execute_with_checkpoint
from utils.loader import Timer
from utils.common import get_paths_helper, class_iter, str_for_timer, get_window_size, get_number_of_windows_by_class

SCRIPT_NAME = basename(__file__)

def plot_num_of_snp_per_class(options):
    paths_helper = get_paths_helper(options.dataset_name)
    os.makedirs(f'{paths_helper.summary_dir}num_of_snps/', exist_ok=True)

    window_size = get_window_size(paths_helper)
    windows2count = get_number_of_windows_by_class(paths_helper)

    num_of_snps = {'mac': [], 'maf': []}
    for cls in tqdm(list(class_iter(options))):
        num_of_snps[cls.mac_maf].append(int(windows2count[cls.name]) * window_size)
    for mac_maf in ["mac", "maf"]:
        num_of_snps[mac_maf] = np.array(num_of_snps[mac_maf])
        plot_per_class(options, mac_maf, values=num_of_snps[mac_maf], std=None, scats=None, colors=['tab:blue'],
                       labels=['Num of SNPs'],
                       title='site frequency spectrum (SFS)',
                       output=f'{paths_helper.summary_dir}num_of_snps/{mac_maf}.svg',
                       polynomials=None,
                       log_scale=True)


def main(options):
    with Timer(f"Per class sum all windows on {str_for_timer(options)}"):
        is_success, msg = execute_with_checkpoint(plot_num_of_snp_per_class, SCRIPT_NAME, options)
    return is_success