# Run nmi on all outputs of netstruct w.r.t ground truth
# python3 4_run_nmi.py 2 18 1 49 0 499 mac_2-18_maf_1-49_windows_0-499 W_1_D_0_Min_5_SS_0.001_B_1.0

import os
import subprocess
import sys
from os.path import dirname, abspath

from steps.s6_compare_to_random_pst.nmi_helper import get_tree_path, collect_all_nodes_if_needed, run_nmi

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.common import get_paths_helper, args_parser
from utils.loader import Timer


def run_nmi_on_all(options):
    mac_min_range, mac_max_range = options.mac
    maf_min_range, maf_max_range = options.maf

    # prepare paths
    paths_helper = get_paths_helper(options.dataset_name)
    ns_base_dir = paths_helper.net_struct_dir
    nmi_output_dir = paths_helper.nmi_dir
    os.makedirs(nmi_output_dir, exist_ok=True)

    # ground truth (gt) files
    gt_base_dir = f'{ns_base_dir}all_mac_{mac_min_range}-{mac_max_range}_maf_{maf_min_range}-{maf_max_range}/'
    gt_dir = get_tree_path(gt_base_dir, options)

    gt_leafs_overlap = f'{gt_dir}2_Leafs_WithOverlap.txt'
    gt_leafs_no_overlap = f'{gt_dir}2_Leafs_NoOverlap.txt'
    gt_all_nodes = collect_all_nodes_if_needed(gt_dir)

    # go over classes
    for mac_maf in ['mac', 'maf']:
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        if min_range > 0:
            print(f'go over {mac_maf} values: [{min_range},{max_range}]')
            for val in range(min_range, max_range + 1):
                # in maf we take 0.x
                if not is_mac:
                    val = f'{val * 1.0 / 100}'
                class_ns_dir = get_tree_path(f'{ns_base_dir}{mac_maf}_{val}_all/', options)
                class_leafs_overlap = f'{class_ns_dir}2_Leafs_WithOverlap.txt'
                class_leafs_no_overlap = f'{class_ns_dir}2_Leafs_NoOverlap.txt'
                class_all_nodes = collect_all_nodes_if_needed(class_ns_dir)

                class_nmi_output = f'{nmi_output_dir}{mac_maf}_{val}_all/'
                os.makedirs(class_nmi_output, exist_ok=True)
                class_step_size_nmi_output = f'{class_nmi_output}step_{options.ns_ss}/'


                # calc nmi
                run_nmi(options, gt_leafs_overlap, class_leafs_overlap, class_step_size_nmi_output + 'Leaves_WithOverlap.txt')

                run_nmi(options, gt_leafs_no_overlap, class_leafs_no_overlap, class_step_size_nmi_output + 'Leaves_NoOverlap.txt')

                run_nmi(options, gt_all_nodes, class_all_nodes, class_step_size_nmi_output + 'AllNodes.txt')


def main(options):
    with Timer(f"run nmi with {options}"):
        run_nmi_on_all(options)


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
