import os
import subprocess
import sys
from os.path import dirname, abspath

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.common import get_paths_helper


def get_tree_path(tree_base_dir, options):
    assert os.path.isdir(tree_base_dir), f"The tree dir path is not exists: {tree_base_dir}"
    tree_dirs = os.listdir(tree_base_dir)
    assert len(tree_dirs) > 0, f"No trees were found in {tree_base_dir}, empty dir"
    gt_right_configured_dirs = [path for path in tree_dirs if f'SS_{options.ns_ss}' in path]
    assert len(gt_right_configured_dirs) > 0, f"No trees were found with step size {options.ns_ss}"
    assert len(gt_right_configured_dirs) < 2, f"More than 1 tree were found with step size {options.ns_ss}"
    return tree_base_dir + gt_right_configured_dirs[0]


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
        print(f'All nodes exists: {all_nodes_path}')
        return all_nodes_path

    # look for community files in the folder, append them to the all_nodes_path file
    with open(all_nodes_path, 'a') as all_nodes_file:
        for f in os.listdir(folder):
            if f.endswith('_C.txt'):
                with open(folder + f) as comm_file:
                    all_nodes_file.write(comm_file.read())
    return all_nodes_path

