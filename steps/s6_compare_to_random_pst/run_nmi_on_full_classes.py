# Run nmi on all outputs of netstruct w.r.t ground truth


import sys
from os.path import dirname, abspath, basename

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.checkpoint_helper import execute_with_checkpoint
from steps.s6_compare_to_random_pst.nmi_helper import get_tree_path, collect_all_nodes_if_needed, run_nmi, \
    prepare_inputs_and_gt, run_all_types_nmi


from utils.common import args_parser
from utils.loader import Timer
SCRIPT_NAME = basename(__file__)


def run_nmi_on_all(options):
    mac_min_range, mac_max_range = options.mac
    maf_min_range, maf_max_range = options.maf
    gt_all_nodes, gt_leafs_no_overlap, gt_leafs_overlap, nmi_output_dir, ns_base_dir = prepare_inputs_and_gt(options)

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
                class_name = f"{mac_maf}_{val}"
                run_all_types_nmi(gt_all_nodes, gt_leafs_no_overlap, gt_leafs_overlap, class_name, nmi_output_dir,
                                  f'{ns_base_dir}{class_name}/', options, 'all')


def main(options):
    with Timer(f"run nmi with {options}"):
        is_executed, msg = execute_with_checkpoint(run_nmi_on_all, SCRIPT_NAME, options)
        print(msg)
    return is_executed


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
