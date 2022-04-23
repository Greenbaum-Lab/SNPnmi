#!/usr/bin/env python

import sys
from os.path import dirname, abspath, basename


root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from steps.s7_join_to_summary import collect_tree_heights
from notebooks import number_of_snp_per_class, plot_nmi_scores
from utils.checkpoint_helper import execute_with_checkpoint
from utils.common import str_for_timer, args_parser
from utils.loader import Timer


def main(options):
    with Timer(f"Plot all plots with {str_for_timer(options)}"):

        is_success1, msg1 = execute_with_checkpoint(number_of_snp_per_class.main, 'number_of_snp_per_clas', options)
        print(msg1)
        is_success2, msg2 = execute_with_checkpoint(plot_nmi_scores.main, 'plot_nmi_scores', options)
        print(msg2)
        is_success3, msg3 = execute_with_checkpoint(collect_tree_heights.main, 'collect_tree_heights', options)
        print(msg3)
    return is_success1 and is_success2 and is_success3


if __name__ == '__main__':
    arguments = args_parser()
    main(arguments)
