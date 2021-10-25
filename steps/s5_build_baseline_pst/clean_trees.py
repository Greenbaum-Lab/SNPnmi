import sys
from os.path import dirname, abspath, basename

from steps.s5_build_baseline_pst.per_class_sum_n_windows import load_hash_data

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.checkpoint_helper import execute_with_checkpoint
from steps.s5_build_baseline_pst.submit_many_netstructs_based_on_fix_size import get_hashes_for_computed_trees
from steps.s6_compare_to_random_pst.nmi_helper import prepare_inputs_and_gt, run_all_types_nmi

from utils.common import get_paths_helper, args_parser
from utils.loader import Timer


def delete_trees_per_class(options, paths_helper, class_name):
    ns_dir = paths_helper.net_struct_dir + f'{class_name}/'
    sim_dir = paths_helper.similarity_by_class_folder_template.format(class_name=class_name)
    hash_file = paths_helper.hash_windows_list_template.format(class_name=class_name)
    hash_dict = load_hash_data(hash_file)
    for k, v in hash_dict.itmes():


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
                delete_trees_per_class(options, paths_helper, class_name)

def main(options):
    pass


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
