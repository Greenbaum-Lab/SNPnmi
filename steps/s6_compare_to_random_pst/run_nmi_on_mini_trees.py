import sys
from os.path import dirname, abspath, basename

from tqdm import tqdm

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.checkpoint_helper import execute_with_checkpoint
from steps.s5_build_baseline_pst.submit_many_netstructs_based_on_fix_size import get_hashes_for_computed_trees
from steps.s6_compare_to_random_pst.nmi_helper import prepare_inputs_and_gt, run_all_types_nmi, \
    check_if_nmi_was_computed

from utils.common import get_paths_helper, args_parser, get_window_size, class_iter
from utils.loader import Timer

SCRIPT_NAME = basename(__file__)


def compute_nmi_scores_per_class(options, class_name, paths_helper, num_of_winds):
    num_of_desired_trees = options.args[1]
    hashes_of_fit_trees = get_hashes_for_computed_trees(options, paths_helper, class_name, num_of_winds)
    num_of_trees = len(hashes_of_fit_trees)
    assert num_of_trees >= num_of_desired_trees, f"There are only {num_of_trees} trees for class {class_name}"
    not_computed_trees = check_if_nmi_was_computed(options, paths_helper, class_name, hashes_of_fit_trees)
    num_of_trees_to_run = max(num_of_desired_trees - (num_of_trees - len(not_computed_trees)), 0)
    gt_all_nodes, gt_leafs_no_overlap, gt_leafs_overlap, ns_base_dir, paths_helper = prepare_inputs_and_gt(options)
    nmi_output_dir = paths_helper.nmi_class_template.format(class_name=class_name)

    for hash_tree in tqdm(not_computed_trees[:num_of_trees_to_run], leave=False):
        run_all_types_nmi(gt_all_nodes, gt_leafs_no_overlap, gt_leafs_overlap, class_name, nmi_output_dir,
                          paths_helper.net_struct_dir_class.format(class_name=class_name), options, hash_tree)


def run_nmi_on_classes_all_trees(options):
    paths_helper = get_paths_helper(options.dataset_name)
    window_size = get_window_size(paths_helper)
    data_size = int(options.args[0])
    assert data_size // window_size == data_size / window_size, "data size is not dividable in window size"
    num_of_windows = data_size // window_size
    for cls in tqdm(list(class_iter(options))):
        compute_nmi_scores_per_class(options, cls.name, paths_helper, num_of_windows)
    return True


def main(options):
    with Timer(f"run nmi with {options}"):
        is_success, msg = execute_with_checkpoint(run_nmi_on_classes_all_trees, SCRIPT_NAME, options)
        print(msg)
    return is_success


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
