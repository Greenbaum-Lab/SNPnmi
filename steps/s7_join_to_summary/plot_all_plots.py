#!/usr/bin/env python
import json
import os
import sys
from os.path import dirname, abspath, basename
import matplotlib.pyplot as plt
import numpy as np


root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from notebooks import number_of_snp_per_class, plot_nmi_scores
from utils.checkpoint_helper import execute_with_checkpoint
from utils.common import str_for_timer, get_paths_helper, args_parser
from utils.loader import Timer

def main(options):
    with Timer(f"Plot all plots with {str_for_timer(options)}"):

        is_success1, msg1 = execute_with_checkpoint(number_of_snp_per_class.main, 'number_of_snp_per_clas', options)
        print(msg1)
        is_success2, msg2 = execute_with_checkpoint(plot_nmi_scores.main, 'plot_nmi_scores', options)
        print(msg2)
        is_success3, msg3 = execute_with_checkpoint(collect_num_of_nodes.main, 'collect_num_of_nodes', options)
        print(msg3)
    return is_success1 and is_success2 and is_success3


if __name__ == '__main__':
    arguments = args_parser()
    main(arguments)
