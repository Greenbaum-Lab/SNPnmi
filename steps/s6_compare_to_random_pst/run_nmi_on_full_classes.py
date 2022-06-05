# Run nmi on all outputs of netstruct w.r.t ground truth

import sys
from os.path import dirname, abspath, basename
from tqdm import tqdm

from steps.s6_compare_to_random_pst.submit_run_nmi import get_gt_path_dictionary
from utils.checkpoint_helper import execute_with_checkpoint
from steps.s6_compare_to_random_pst.nmi_helper import prepare_inputs_and_gt, run_all_types_nmi, \
    collect_all_nodes_if_needed
from utils.common import args_parser, class_iter, get_paths_helper
from utils.loader import Timer
SCRIPT_NAME = basename(__file__)


def run_nmi_on_all(options):
    paths_helper = get_paths_helper(options.dataset_name)
    gt_paths = get_gt_path_dictionary(options, paths_helper)
    for gt_name, gt_path in gt_paths.items():
        gt_leafs_overlap = f'{gt_path}2_Leafs_WithOverlap.txt'
        gt_all_nodes = collect_all_nodes_if_needed(options, gt_path)

        # go over classes
        for cls in tqdm(list(class_iter(options))):
            nmi_output_dir = paths_helper.nmi_class_template.format(gt_name=gt_name, class_name=cls.name)
            run_all_types_nmi(gt_all_nodes, gt_leafs_overlap, cls.name, nmi_output_dir,
                              f'{paths_helper.net_struct_dir_class.format(class_name=cls.name)}', options, 'all')
    return True

def main(options):
    with Timer(f"run nmi with {options}"):
        is_success, msg = execute_with_checkpoint(run_nmi_on_all, SCRIPT_NAME, options)
        print(msg)
    return is_success


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
