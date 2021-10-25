import sys
from os.path import dirname, abspath, basename

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.checkpoint_helper import execute_with_checkpoint
from steps.s5_build_baseline_pst.submit_many_netstructs_based_on_fix_size import get_hashes_for_computed_trees
from steps.s6_compare_to_random_pst.nmi_helper import prepare_inputs_and_gt, run_all_types_nmi

from utils.common import get_paths_helper, args_parser
from utils.loader import Timer
SCRIPT_NAME = basename(__file__)


def compute_nmi_scores_per_class(options, class_name, paths_helper, num_of_winds):
    num_of_desired_trees = options.args[1]
    hashes_of_fit_trees = get_hashes_for_computed_trees(options, paths_helper, class_name, num_of_winds)
    num_of_trees = len(hashes_of_fit_trees)
    assert num_of_trees >= num_of_desired_trees, f"There are only {num_of_trees} trees for class {class_name}"
    gt_all_nodes, gt_leafs_no_overlap, gt_leafs_overlap, nmi_output_dir, ns_base_dir = prepare_inputs_and_gt(options)

    for hash_tree in hashes_of_fit_trees[:num_of_desired_trees]:
        run_all_types_nmi(gt_all_nodes, gt_leafs_no_overlap, gt_leafs_overlap, class_name, nmi_output_dir, ns_base_dir,
                          options, hash_tree)


def run_nmi_on_classes_all_trees(options):
    paths_helper = get_paths_helper(options.dataset_name)
    mac_min_range, mac_max_range = options.mac
    maf_min_range, maf_max_range = options.maf
    with open(paths_helper.windows_dir + 'window_size.txt', 'r') as f:
        window_size = int(f.read())
    data_size = options.args[0]
    assert data_size // window_size == data_size/window_size, "data size is not dividable in window size"
    num_of_windows = data_size // window_size
    print(num_of_windows)
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
                class_name = f"{mac_maf}_{val}"
                compute_nmi_scores_per_class(options, class_name, paths_helper, num_of_windows)


def main(options):
    with Timer(f"run nmi with {options}"):
        is_executed, msg = execute_with_checkpoint(run_nmi_on_classes_all_trees, SCRIPT_NAME, options)
        print(msg)
    return is_executed

if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
