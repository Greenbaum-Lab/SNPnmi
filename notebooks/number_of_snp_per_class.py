#!/usr/bin/env python
import json
import os
import sys
from os.path import dirname, abspath, basename
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)


from utils.checkpoint_helper import execute_with_checkpoint
from utils.loader import Timer
from utils.common import get_paths_helper, class_iter, str_for_timer, get_window_size, get_number_of_windows_by_class

SCRIPT_NAME = basename(__file__)

def plot_num_of_snp_per_class(options):
    paths_helper = get_paths_helper(options.dataset_name)
    os.makedirs(f'{paths_helper.summary_dir}num_of_snps/', exist_ok=True)
    mac_min_range, mac_max_range = options.mac
    maf_min_range, maf_max_range = options.maf


    window_size = get_window_size(paths_helper)
    windows2count = get_number_of_windows_by_class(paths_helper)

    num_of_snps = {'mac': [], 'maf': []}
    for cls in tqdm(list(class_iter(options))):
        num_of_snps[cls.mac_maf].append(int(windows2count[cls.name]) * window_size)
    for mac_maf in ["mac", "maf"]:
        class_names = mac_class_names if mac_maf == ' maf' else maf_class_names
        plt.plot(class_names, num_of_snps[mac_maf])
        plt.yscale('log')
        plt.xlabel(f"{mac_maf}", fontsize=16)
        plt.xticks(fontsize=10)
        plt.yticks(fontsize=10)
        plt.title('site frequency spectrum (SFS)', fontsize=18)
        plt.savefig(f'{paths_helper.summary_dir}num_of_snps/{mac_maf}.svg')
        plt.clf()


def main(options):
    with Timer(f"Per class sum all windows on {str_for_timer(options)}"):
        is_success, msg = execute_with_checkpoint(plot_num_of_snp_per_class, SCRIPT_NAME, options)
    return is_success