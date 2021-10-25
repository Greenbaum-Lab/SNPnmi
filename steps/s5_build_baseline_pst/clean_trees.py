import sys
from os.path import dirname, abspath, basename

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.checkpoint_helper import execute_with_checkpoint
from steps.s5_build_baseline_pst.submit_many_netstructs_based_on_fix_size import get_hashes_for_computed_trees
from steps.s6_compare_to_random_pst.nmi_helper import prepare_inputs_and_gt, run_all_types_nmi

from utils.common import get_paths_helper, args_parser
from utils.loader import Timer


def main(options):
    pass


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
