#!/sci/labs/gilig/shahar.mazie/icore-data/snpnmi_venv/bin/python
# given a step number and params, will run the step.
import subprocess
import sys
import os
from os.path import dirname
from time import time

root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)

from utils.scripts import sync_to_gdrive
from utils.loader import Timer
from steps.s1_get_data import get_data, get_vcfs_stats, fix_ref_allele
from steps.s2_split_vcfs_by_class import submit_split_vcfs_by_class, collect_split_vcf_stats
from steps.s3_split_to_windows import submit_prepare_for_split_to_windows
from steps.s3_split_to_windows import submit_split_chr_class_to_windows
from steps.s4_calc_similarity import submit_merge_all_chrs_to_class_windows
from steps.s5_build_baseline_pst import submit_per_class_sum_all_windows,\
    sum_similarities_from_all_classes_and_run_netstrcut, submit_netstruct_per_class, \
    submit_many_netstructs_based_on_fix_size
from steps.s6_compare_to_random_pst import run_nmi_on_full_classes, submit_run_nmi
from steps.s7_join_to_summary import run_all_summaries, plot_all_plots
from utils.checkpoint_helper import execute_with_checkpoint
from utils.common import args_parser, str_for_timer, add_time_to_controller_file, get_paths_helper


step_to_func_and_name = {
    "1.1": (get_data.main, 'get_data'),
    "1.2": (get_vcfs_stats.main, 'get_vcfs_stats'),
    "1.3": (fix_ref_allele.main, 'fix_vcf_ref_allele'),
    "2.1": (submit_split_vcfs_by_class.main, 'submit_split_vcfs_by_class'),
    "2.2": (collect_split_vcf_stats.main, 'collect_split_vcf_stats'),
    "3.1": (submit_prepare_for_split_to_windows.main, 'submit_prepare_for_split_to_windows'),
    "3.2": (submit_split_chr_class_to_windows.main, 'submit_split_chr_class_to_windows'),
    "4.1": (submit_merge_all_chrs_to_class_windows.main, 'submit_merge_all_chrs_to_class_windows'),
    "5.1": (submit_per_class_sum_all_windows.main, 'submit_per_class_sum_all_windows'),
    "5.2": (sum_similarities_from_all_classes_and_run_netstrcut.main, 'sum_similarities_from_all_classes_and_run_netstrcut'),
    "5.3": (submit_many_netstructs_based_on_fix_size.main, 'submit_many_netstructs_based_on_fix_size'),
    "6.1": (run_nmi_on_full_classes.main, 'run_nmi_on_full_classes'),
    "6.2": (submit_run_nmi.main, 'run_nmi_on_mini_trees'),
    "7.1": (run_all_summaries.main, 'run_all_summaries'),
    "7.2": (plot_all_plots.main, 'plot_all_plots')
}

def run_step(options, step, use_checkpoint=True):
    func, step_name = step_to_func_and_name[step]
    if not use_checkpoint:
        return func(options)
    # note that we use the step number and name for the checkpont, so this will only not run if we used runner in the past.
    is_done, msg = execute_with_checkpoint(func, options.step + step_name, options)
    print(msg)
    return is_done


def run_all_pipeline(options):
    def set_options_args(_options, _step, _orig_args):
        if _step == '1.2':
            _options.args = ['freq']
        elif _step == '1.3':
            _options.args = []
        elif _step == '3.1':
            _options.args = [_orig_args[0]]   # window size

        elif _step == '4.1':
            _options.args = [_orig_args[1]]  # num_of_winds_per_job
        else:
            _options.args = _orig_args
        return _options

    paths_helper = get_paths_helper(options.dataset_name)
    orig_args = options.args
    s_lst = ['1.1', '1.2', '1.3', '2.1', '2.2', '3.1', '3.2', '4.1', '5.1', '5.2', '5.3', '6.1', '6.2', '7.1', '7.2']
    for step in s_lst:
        start_step_time = time()
        print(f'start step {step}')
        options = set_options_args(options, step, orig_args)
        options.step = step
        success_run = run_step(options, options.step)
        assert success_run, f"Failed in step {step}"
        if time() - start_step_time > 1:   # > 1 second means there was no checkpoint
            add_time_to_controller_file(paths_helper.data_dir, (time() - start_step_time), step)

    sync_to_gdrive.sync_cluster_to_google_drive(source_file=paths_helper.summary_dir,
                                                dest_dir=f'remote:gili_lab/vcf/{options.dataset_name}/',
                                                dest_file=None)


def runner(options):
    with Timer(f"Runner time with {str_for_timer(options)}"):
        step = options.step
        dataset_name = options.dataset_name
        print(f'Argument List: {step}, {dataset_name}, {str_for_timer(options)}')

        is_executed = run_step(options, step)
        print(f'is executed: {is_executed}')

# python3 runner.py -d sim_dip_v1 --args 100,3000 --run_all

#  python3 runner.py -d hgdp -s 1.1

#  python3 runner.py -d hgdp -s 1.2 --args freq

#  python3 runner.py -s 2.1 -d hgdp

#  python3 runner.py  -s 2.2 -d hgdp

#  python3 runner.py -s 3.1 -d hgdp --args 100  (# window size)

#  python3 runner.py -s 3.2 -d hgdp

#  python3 runner.py -s 3.3 -d hgdp --args 2000  (# num of windows per job)

#  python3 runner.py -s 4.1 -d hgdp

#  python3 runner.py -s 5.1 -d hgdp

#  python3 runner.py -s 5.2 -d hgdp

#  python3 runner.py -s 5.3-d hgdp --args 1000,3  (# num of SNPs per tree, num of trees)

#  python3 runner.py -s 6.1 -d hgdp

#  python3 runner.py -s 6.2 -d hgdp --args 1000,3   (# num of SNPs per tree, num of trees)



if __name__ == "__main__":
    arguments = args_parser()
    if arguments.run_all:
        run_all_pipeline(arguments)
    else:
        runner(arguments)
