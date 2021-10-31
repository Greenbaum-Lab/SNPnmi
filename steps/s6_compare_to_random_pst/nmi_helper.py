import os
import subprocess
import sys
from os.path import dirname, abspath

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.common import get_paths_helper

def check_if_nmi_was_computed(options, paths_helper, class_name, hash_trees):
    NMI_FILES_NAMES = ['AllNodes.txt', 'Leaves_NoOverlap.txt', 'Leaves_WithOverlap.txt']
    not_computed_nmi = []
    for tree in hash_trees:
        nmi_tree_dir = paths_helper.nmi_tree_template.format(class_name=class_name, tree_hash=tree, ns_ss=options.ns_ss)
        if not os.path.exists(nmi_tree_dir):
            not_computed_nmi.append(tree)
            continue
        for file_name in NMI_FILES_NAMES:
            if not os.path.exists(nmi_tree_dir + file_name):
                not_computed_nmi.append(tree)
                continue

    return not_computed_nmi

def run_all_types_nmi(gt_all_nodes, gt_leafs_no_overlap, gt_leafs_overlap, class_name, nmi_output_dir, ns_base_dir,
                      options, tree_hash):
    class_ns_dir = get_tree_path(f'{ns_base_dir}{class_name}_{tree_hash}/', options)
    class_leafs_overlap = f'{class_ns_dir}2_Leafs_WithOverlap.txt'
    class_leafs_no_overlap = f'{class_ns_dir}2_Leafs_NoOverlap.txt'
    class_all_nodes = collect_all_nodes_if_needed(class_ns_dir)
    class_nmi_output = f'{nmi_output_dir}{class_name}_{tree_hash}/'
    os.makedirs(class_nmi_output, exist_ok=True)
    class_step_size_nmi_output = f'{class_nmi_output}step_{options.ns_ss}/'
    os.makedirs(class_step_size_nmi_output, exist_ok=True)
    # calc nmi
    run_nmi(options, gt_leafs_overlap, class_leafs_overlap, class_step_size_nmi_output + 'Leaves_WithOverlap.txt')
    run_nmi(options, gt_leafs_no_overlap, class_leafs_no_overlap, class_step_size_nmi_output + 'Leaves_NoOverlap.txt')
    run_nmi(options, gt_all_nodes, class_all_nodes, class_step_size_nmi_output + 'AllNodes.txt')


def prepare_inputs_and_gt(options):
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
    return gt_all_nodes, gt_leafs_no_overlap, gt_leafs_overlap, nmi_output_dir, ns_base_dir


def get_tree_path(tree_base_dir, options):
    assert os.path.isdir(tree_base_dir), f"The tree dir path is not exists: {tree_base_dir}"
    tree_dirs = os.listdir(tree_base_dir)
    assert len(tree_dirs) > 0, f"No trees were found in {tree_base_dir}, empty dir"
    gt_right_configured_dirs = [path for path in tree_dirs if f'SS_{options.ns_ss}' in path]
    assert len(gt_right_configured_dirs) > 0, f"No trees were found with step size {options.ns_ss}"
    assert len(gt_right_configured_dirs) < 2, f"More than 1 tree were found with step size {options.ns_ss}"
    return tree_base_dir + gt_right_configured_dirs[0] + '/'


def run_nmi(options, input1, input2, output_path):
    paths_helper = get_paths_helper(options.dataset_name)
    nmi_path = paths_helper.nmi_exe
    nmi_process = subprocess.Popen((nmi_path, input1, input2), stdout=subprocess.PIPE)
    nmi_output = nmi_process.stdout.read().decode()
    with open(output_path, 'w') as f:
        f.write(nmi_output)


def collect_all_nodes_if_needed(folder):
    # only if output file does not exist we will create it
    all_nodes_path = f'{folder}AllNodes.txt'
    if os.path.exists(all_nodes_path):
        return all_nodes_path

    # look for community files in the folder, append them to the all_nodes_path file
    with open(all_nodes_path, 'a') as all_nodes_file:
        for f in os.listdir(folder):
            if f.endswith('_C.txt'):
                with open(folder + f) as comm_file:
                    all_nodes_file.write(comm_file.read())
    return all_nodes_path

