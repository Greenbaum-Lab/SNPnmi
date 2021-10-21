import sys
from os.path import dirname, abspath

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from steps.s5_build_baseline_pst.submit_many_netstructs_based_on_fix_size import get_hashes_for_computed_trees
from steps.s6_compare_to_random_pst.nmi_helper import prepare_inputs_and_gt, run_all_types_nmi

from utils.common import get_paths_helper, args_parser
from utils.loader import Timer


def compute_nmi_scores_per_class(options, class_name, paths_helper):
    num_of_winds = options.args[0]
    num_of_desired_trees = options.args[1]
    hashes_of_fit_trees = get_hashes_for_computed_trees(paths_helper, class_name, num_of_winds)
    num_of_trees = len(hashes_of_fit_trees)
    assert num_of_trees >= num_of_desired_trees, f"There are only {num_of_trees} trees for class {class_name}"
    gt_all_nodes, gt_leafs_no_overlap, gt_leafs_overlap, nmi_output_dir, ns_base_dir = prepare_inputs_and_gt(options)

    for hash_tree in hashes_of_fit_trees:
        run_all_types_nmi(gt_all_nodes, gt_leafs_no_overlap, gt_leafs_overlap, class_name, nmi_output_dir, ns_base_dir,
                          options, hash_tree)


def run_nmi_on_classes_all_trees(options):
    paths_helper = get_paths_helper(options.dataset_name)
    mac_min_range, mac_max_range = options.mac
    maf_min_range, maf_max_range = options.maf
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
                compute_nmi_scores_per_class(options, class_name, paths_helper)


def main(options):
    with Timer(f"run nmi with {options}"):
        run_nmi_on_classes_all_trees(options)


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
