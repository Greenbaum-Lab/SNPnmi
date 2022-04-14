#!/usr/bin/python3

from os.path import dirname, abspath
import sys
import os


root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from steps.s7_join_to_summary import collect_nmi, collect_tree_heights, collect_num_of_nodes
from utils.checkpoint_helper import execute_with_checkpoint
from utils.common import args_parser, str_for_timer, get_paths_helper
from utils.loader import Timer


def main(options):
    with Timer(f"Summarize all runs {str_for_timer(options)}"):
        paths_helper = get_paths_helper(options.dataset_name)
        os.makedirs(paths_helper.summary_dir, exist_ok=True)
        is_success1, msg1 = execute_with_checkpoint(collect_nmi.main, 'collect_nmi', options)
        print(msg1)
        is_success2, msg2 = execute_with_checkpoint(collect_tree_heights.main, 'collect_tree_heights', options)
        print(msg2)
        is_success3, msg3 = execute_with_checkpoint(collect_num_of_nodes.main, 'collect_num_of_nodes', options)
        print(msg3)
    return is_success1 and is_success2 and is_success3


if __name__ == '__main__':
    arguments = args_parser()
    main(arguments)
