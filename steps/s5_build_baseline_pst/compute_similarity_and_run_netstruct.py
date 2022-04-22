import subprocess
import os
import sys
from os.path import dirname, abspath


root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from steps.s6_compare_to_random_pst.nmi_helper import collect_all_nodes_if_needed
from utils.netstrcut_helper import build_netstruct_cmd
from steps.s5_build_baseline_pst.per_class_sum_n_windows import sum_windows
from utils.loader import Timer
from utils.common import get_paths_helper, args_parser, load_dict_from_json, delete_extra_files

job_type = "mini_net-struct"


def run_net_struct(options, job_type, similarity_matrix_path, output_dir):
    # create output folders
    paths_helper = get_paths_helper(options.dataset_name)
    os.makedirs(dirname(paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name='dummy')),
                exist_ok=True)

    net_struct_cmd = build_netstruct_cmd(options, similarity_matrix_path, output_dir, options.ns_ss)
    with open(paths_helper.garbage, "wb") as garbage_output:
        subprocess.run([paths_helper.submit_helper, net_struct_cmd], stdout=garbage_output)


def compute_similarity_and_run_net_struct(options, mac_maf, class_val, paths_helper, winds):
    class_name = f'{mac_maf}_{class_val}'
    tree_hash = sum_windows(class_name=class_name, windows_id_list=winds,
                            similarity_window_template=paths_helper.similarity_by_class_and_window_template,
                            count_window_template=paths_helper.count_by_class_and_window_template,
                            output_dir=paths_helper.similarity_by_class_folder_template.format(
                                class_name=class_name), paths_helper=paths_helper)

    similarity_dir = paths_helper.similarity_by_class_folder_template.format(class_name=class_name)
    similarity_edges_file = similarity_dir + f'{class_name}_hash{tree_hash}_edges.txt'
    output_dir = paths_helper.net_struct_dir_class.format(class_name=class_name) + f'{class_name}_{tree_hash}/'
    ns_output_dir_name = f'W_1_D_0_Min_5_SS_{options.ns_ss}_B_1.0/'
    run_net_struct(options, job_type, similarity_edges_file, output_dir)
    collect_all_nodes_if_needed(output_dir + ns_output_dir_name)
    delete_extra_files(output_dir + ns_output_dir_name,
                       ['2_Leafs_WithOverlap.txt', '2_Leafs_NoOverlap.txt', 'AllNodes.txt',
                        f'1_CommAnalysis_dynamic-false_modularity-true_minCommBrake-5_{options.ns_ss}.txt'])


def main(options):
    mac_maf = options.args[0]
    assert mac_maf == 'mac' or mac_maf == 'maf'
    val = options.args[1]
    tree_hash = str(options.args[2])
    class_name = f"{mac_maf}_{val}"
    paths_helper = get_paths_helper(options.dataset_name)
    hash_json = load_dict_from_json(paths_helper.hash_windows_list_template.format(class_name=class_name))
    winds = hash_json[tree_hash]
    compute_similarity_and_run_net_struct(options, mac_maf, val, paths_helper, winds)

    return True


if __name__ == "__main__":
    arguments = args_parser()
    with Timer(f"compute similarity and run net struct with {arguments.args}"):
        main(arguments)
