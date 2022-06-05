#!/sci/labs/gilig/shahar.mazie/icore-data/snpnmi_venv/bin/python
import subprocess
import sys
from os.path import basename, dirname, abspath

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.config import get_cluster_code_folder
from utils.netstrcut_helper import is_tree_exists
from utils.common import get_paths_helper, args_parser
from utils.checkpoint_helper import execute_with_checkpoint
from utils.loader import Timer

path_to_python_script_to_run = f'{get_cluster_code_folder()}snpnmi/steps/s5_build_baseline_pst/compute_similarity_and_run_netstruct.py'
job_type = "mini_net-struct"
SCRIPT_NAME = basename(__file__)


def submit_mini_net_struct_trees(options):
    paths_helper = get_paths_helper(options.dataset_name)
    args = options.args
    mac_maf = args[0]
    class_val = args[1]
    class_name = f'{mac_maf}_{class_val}'
    tree_hashes = args[2:]
    for tree_hash in tree_hashes:
        output_dir = paths_helper.net_struct_dir_class.format(class_name=class_name) + f'{class_name}_{tree_hash}/'
        job_long_name = f'{class_name}_hash{tree_hash}_ns_{options.ns_ss}_weighted_true'
        job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                job_name=job_long_name)
        job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                                job_name=job_long_name)
        script_args = f'-d {options.dataset_name} --args {mac_maf},{class_val},{tree_hash} --ns_ss {options.ns_ss}' \
                      f' --min_pop_size {options.min_pop_size}'
        if is_tree_exists(options, output_dir, job_stderr_file):
            print(f"Tree exists already for {job_long_name} with step size {options.ns_ss} - NOT RUNNING!")
            continue
        cmd_list = ['python3', path_to_python_script_to_run] + script_args.split(' ')
        with open(job_stdout_file, "wb") as out, open(job_stderr_file, "wb") as err:
            subprocess.run(cmd_list, stdout=out, stderr=err)
            print(cmd_list)
        print(f"Done with hash {tree_hash}")


def main(options):
    with Timer(f"run net-struct multiple times with {options}"):
        is_executed, msg = execute_with_checkpoint(submit_mini_net_struct_trees, SCRIPT_NAME, options)
        print(msg)
    return is_executed


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
