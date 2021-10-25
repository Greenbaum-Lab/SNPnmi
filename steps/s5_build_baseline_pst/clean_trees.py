import os
import shutil
import sys
from os.path import dirname, abspath, basename


root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from steps.s5_build_baseline_pst.per_class_sum_n_windows import load_hash_data
from utils.common import get_paths_helper, args_parser
from utils.loader import Timer


def track_invalid_hashes_per_class(options, paths_helper, class_name):
    ns_dir = paths_helper.net_struct_dir + f'{class_name}'
    sim_dir = paths_helper.similarity_by_class_folder_template.format(class_name=class_name)
    hash_file = paths_helper.hash_windows_list_template.format(class_name=class_name)
    hash_dict = load_hash_data(hash_file)
    log_file_template = paths_helper.logs_cluster_jobs_stderr_template.format(job_type='mini_net-struct')
    invalid_hashes = []
    for k in hash_dict.keys():
        job_name = f"{class_name}_hash{k}_ns_{options.ns_ss}_weighted_true"
        log_file = log_file_template.format(job_name=job_name)
        if not os.path.exists(log_file) or os.stat(log_file).st_size > 0:
            invalid_hashes.append(k)
        elif not os.path.exists(sim_dir + f"{class_name}_hash{k}_edges.txt"):
            invalid_hashes.append(k)
        elif not os.path.isdir(ns_dir + f'_{k}') or not os.listdir(ns_dir + f'_{k}'):
            invalid_hashes.append(k)
    if invalid_hashes:
        print(f"Deleting for class {class_name} the next hashes: {invalid_hashes}")
    return invalid_hashes


def erase_invalid_trees(options, paths_helper, class_name, invalid_hashes):
    ns_dir = paths_helper.net_struct_dir + f'{class_name}'
    sim_dir = paths_helper.similarity_by_class_folder_template.format(class_name=class_name)
    hash_file = paths_helper.hash_windows_list_template.format(class_name=class_name)
    hash_data = load_hash_data(hash_file)
    for k in invalid_hashes:
        del hash_data[k]
        job_name = f"{class_name}_hash{k}_ns_{options.ns_ss}_weighted_true"
        stdout = paths_helper.logs_cluster_jobs_stsdout_template.format(job_type='mini_net-struct', job_name=job_name)
        stderr = paths_helper.logs_cluster_jobs_stderr_template.format(job_type='mini_net-struct', job_name=job_name)
        if os.path.exists(stdout):
            os.remove(stderr)
            os.remove(stdout)
        if os.path.exists(sim_dir + f"{class_name}_hash{k}_count.npy"):
            os.remove(sim_dir + f"{class_name}_hash{k}_count.npy")
        if os.path.exists(sim_dir + f"{class_name}_hash{k}_similarity.npy"):
            os.remove(sim_dir + f"{class_name}_hash{k}_count.npy")
        if os.path.exists(sim_dir + f"{class_name}_hash{k}_edges.txt"):
            os.remove(sim_dir + f"{class_name}_hash{k}_edges.txt")
        ns_path = ns_dir + f"_{k}/"
        if os.path.exists(ns_path):
            shutil.rmtree(ns_path)
    with open(hash_file, "w") as f:
        f.write(hash_data)


def delete_unfinished_trees_and_hashes(options):
    mac_min_range, mac_max_range = options.mac
    maf_min_range, maf_max_range = options.maf
    paths_helper = get_paths_helper(dataset_name=options.dataset_name)
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
                class_name = f'{mac_maf}_{val}'
                invalid_hashes = track_invalid_hashes_per_class(options, paths_helper, class_name)
                if invalid_hashes:
                    erase_invalid_trees(options, paths_helper, class_name, invalid_hashes)


def main(options):
    with Timer(f"Cleaning trees"):
        delete_unfinished_trees_and_hashes(options)


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
