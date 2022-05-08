import json
import os
import sys
from os.path import dirname, abspath


root_path = dirname(dirname(dirname(abspath(__file__))))
from steps.s5_build_baseline_pst.compute_similarity_and_run_netstruct import run_net_struct
sys.path.append(root_path)

from utils.loader import Timer
from utils.common import get_paths_helper, args_parser
from utils.similarity_helper import generate_similarity_matrix


def _get_similarity_per_window_files_names(paths_helper, class_str):
    with open(paths_helper.number_of_windows_per_class_path, 'r') as f:
        num_of_wind_per_class = json.load(f)
    windows_similarity_dir = paths_helper.per_window_similarity.format(class_name=class_str)
    count_similarity_files = os.listdir(windows_similarity_dir)
    assert int(num_of_wind_per_class[class_str]) == len(count_similarity_files) / 2  # count and similarity are diff files
    count_files = [windows_similarity_dir + file for file in count_similarity_files if "count" in file]
    count_files.sort()
    similarity_files = [windows_similarity_dir + file for file in count_similarity_files if "similarity" in file]
    similarity_files.sort()
    assert len(similarity_files) == len(count_files)
    assert len([file for file in count_similarity_files if "similarity" in file and "count" in file]) == 0
    assert len(similarity_files) + len(count_files) == len(count_similarity_files)
    return count_files, similarity_files


def main(options):
    mac_maf = options.args[0]
    assert mac_maf == 'mac' or mac_maf == 'maf'
    val = options.args[1]
    class_name = f"{mac_maf}_{val}"

    # Prepare paths
    paths_helper = get_paths_helper(options.dataset_name)
    output_dir = paths_helper.similarity_by_class_folder_template.format(class_name=class_name)
    count_files, similarity_files = _get_similarity_per_window_files_names(paths_helper, class_name)

    print('output_dir', output_dir)

    generate_similarity_matrix(similarity_files, count_files, output_dir, f'{output_dir}{class_name}_all',
                               save_np=True, save_edges=True)

    # Run net struct for all the class
    edge_file = f'{output_dir}{class_name}_all_edges.txt'
    output_dir = f'{paths_helper.net_struct_dir_class.format(class_name=class_name)}/{class_name}_all/'
    run_net_struct(options, 'class_ns', edge_file, output_dir=output_dir)

if __name__ == "__main__":
    arguments = args_parser()
    with Timer(f"per_class_sum_all_windows with {arguments.args}"):
        main(arguments)

