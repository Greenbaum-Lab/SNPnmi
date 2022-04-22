#!/usr/bin/env
# python3 utils/scripts/clean_trees.py -d hgdp

import os
import shutil
import json
import sys
from os.path import dirname, abspath, basename

from tqdm import tqdm

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.common import get_paths_helper, args_parser, load_dict_from_json, class_iter
from utils.loader import Timer


def track_invalid_hashes_per_class(options, paths_helper, class_name):
    ns_dir = paths_helper.net_struct_dir_class.format(class_name=class_name)
    sim_dir = paths_helper.similarity_by_class_folder_template.format(class_name=class_name)
    hash_file = paths_helper.hash_windows_list_template.format(class_name=class_name)
    hash_list_dict = load_dict_from_json(hash_file)
    invalid_hashes = []
    for k in hash_list_dict.keys():
        job_name = f"{class_name}_hash{k}_ns_{options.ns_ss}_weighted_true"
        log_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type='mini_net-struct', job_name=job_name)
        if not os.path.exists(log_file) or os.stat(log_file).st_size > 0:
            invalid_hashes.append(k)
        elif not os.path.exists(sim_dir + f"{class_name}_hash{k}_edges.txt"):
            invalid_hashes.append(k)
        elif not os.path.isdir(ns_dir + f'{class_name}_{k}') or not os.listdir(ns_dir + f'{class_name}_{k}'):
            invalid_hashes.append(k)
    if invalid_hashes:
        print(f"Deleting for class {class_name} the next hashes: {invalid_hashes}")
    return invalid_hashes


def erase_invalid_trees(options, paths_helper, class_name, invalid_hashes):
    ns_dir = paths_helper.net_struct_dir_class.format(class_name=class_name)
    sim_dir = paths_helper.similarity_by_class_folder_template.format(class_name=class_name)
    hash_file = paths_helper.hash_windows_list_template.format(class_name=class_name)
    hash_data = load_dict_from_json(hash_file)
    for k in invalid_hashes:
        if k in hash_data.keys():
            del hash_data[k]
        job_name = f"{class_name}_hash{k}_ns_{options.ns_ss}_weighted_true"
        stdout = paths_helper.logs_cluster_jobs_stdout_template.format(job_type='mini_net-struct', job_name=job_name)
        stderr = paths_helper.logs_cluster_jobs_stderr_template.format(job_type='mini_net-struct', job_name=job_name)
        if os.path.exists(stdout):
            os.remove(stderr)
            os.remove(stdout)
        if os.path.exists(sim_dir + f"{class_name}_hash{k}_edges.txt"):
            os.remove(sim_dir + f"{class_name}_hash{k}_edges.txt")
        ns_path = ns_dir + f"{class_name}_{k}/"
        if os.path.exists(ns_path):
            shutil.rmtree(ns_path)
    with open(hash_file, "w") as f:
        json.dump(hash_data, f)


def delete_unfinished_trees_and_hashes(options):
    dry_run = len(options.args) > 0
    paths_helper = get_paths_helper(dataset_name=options.dataset_name)
    num_of_deleted_trees = 0
    for cls in tqdm(list(class_iter(options)),desc='clean trees'):
        invalid_hashes = track_invalid_hashes_per_class(options, paths_helper, cls.name)
        if invalid_hashes:
            num_of_deleted_trees += len(invalid_hashes)
            if not dry_run:
                erase_invalid_trees(options, paths_helper, cls.name, invalid_hashes)
    print(f"Erased {num_of_deleted_trees} trees")

def main(options):
    with Timer(f"Cleaning trees"):
        delete_unfinished_trees_and_hashes(options)


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
