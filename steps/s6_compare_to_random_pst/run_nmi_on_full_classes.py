# Run nmi on all outputs of netstruct w.r.t ground truth

import sys
from os.path import dirname, abspath, basename
from tqdm import tqdm

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.checkpoint_helper import execute_with_checkpoint
from steps.s6_compare_to_random_pst.nmi_helper import prepare_inputs_and_gt, run_all_types_nmi
from utils.common import args_parser, class_iter
from utils.loader import Timer
SCRIPT_NAME = basename(__file__)


def run_nmi_on_all(options):
    gt_all_nodes, gt_leafs_no_overlap, gt_leafs_overlap, ns_base_dir, paths_helper = prepare_inputs_and_gt(options)

    # go over classes
    for cls in tqdm(list(class_iter(options))):
        nmi_output_dir = paths_helper.nmi_class_template.format(class_name=cls.name)
        run_all_types_nmi(gt_all_nodes, gt_leafs_no_overlap, gt_leafs_overlap, cls.name, nmi_output_dir,
                          f'{ns_base_dir}{cls.name}/', options, 'all')


def main(options):
    with Timer(f"run nmi with {options}"):
        is_success, msg = execute_with_checkpoint(run_nmi_on_all, SCRIPT_NAME, options)
        print(msg)
    return is_success


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
