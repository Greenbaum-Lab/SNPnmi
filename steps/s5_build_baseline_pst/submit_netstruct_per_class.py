# python3 submit_netstruct_per_class.py 2 18 1 49 70

import sys
import os
from os.path import dirname, abspath

root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)
from utils.common import get_paths_helper
from utils.validate import _validate_count_dist_file
from utils.netstrcut_helper import submit_netstcut

job_type = 'netstruct_per_class'


def submit_netstruct_for_all(mac_min_range, mac_max_range, maf_min_range, maf_max_range):
    paths_helper = get_paths_helper()
    number_of_submitted_jobs = 0
    # submit one with all data
    job_long_name = f'all_weighted_true'
    job_name = f'ns_all'
    input_name = f'all_mac_{mac_min_range}-{mac_max_range}_maf_{maf_min_range}-{maf_max_range}'
    similarity_matrix_path = paths_helper.dist_folder + input_name + '_norm_dist.tsv.gz'
    output_folder = paths_helper.net_struct_dir + input_name + '/'
    submit_netstcut(job_type, job_long_name, job_name, similarity_matrix_path, output_folder)


def submit_netstruct_per_class(mac_min_range, mac_max_range, maf_min_range, maf_max_range, max_number_of_jobs):
    paths_helper = get_paths_helper('hgdp')
    number_of_submitted_jobs = 0

    # now submit netstruct class by class
    for mac_maf in ['mac', 'maf']:
        netstruct_cmd = None
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        if min_range >= 0:
            print(f'go over {mac_maf} values: [{min_range},{max_range}]')
            for val in range(min_range, max_range + 1):
                if number_of_submitted_jobs == max_number_of_jobs:
                    break
                # in maf we take 0.x
                if not is_mac:
                    val = f'{val * 1.0 / 100}'
                job_long_name = f'{mac_maf}{val}_weighted_true'
                job_name = f'ns_{val}'
                similarity_matrix_path = paths_helper.similarity_dir + f'{mac_maf}_{val}_all_norm_dist.tsv.gz'
                output_folder = paths_helper.net_struct_dir + f'{mac_maf}_{val}_all/'
                submit_netstcut(job_type, job_long_name, job_name, similarity_matrix_path, output_folder)

                if submit_netstcut:
                    number_of_submitted_jobs += 1
                    if number_of_submitted_jobs == max_number_of_jobs:
                        print(f'No more jobs will be submitted.')
                        break


submit_netstruct_per_class(1, 0, 49, 49, 1)

if __name__ == '__Xmain__':
    # by mac
    mac_min_range = int(sys.argv[1])
    mac_max_range = int(sys.argv[2])

    # by maf
    maf_min_range = int(sys.argv[3])
    maf_max_range = int(sys.argv[4])

    # submission details
    # maybe support also these in the future for slices
    # min_window_index =  int(sys.argv[5])
    # max_window_index =  int(sys.argv[6])
    max_number_of_jobs = int(sys.argv[5])

    # print the inputs
    print('mac_min_range', mac_min_range)
    print('mac_max_range', mac_max_range)
    print('maf_min_range', maf_min_range)
    print('maf_max_range', maf_max_range)
    print('max_number_of_jobs', max_number_of_jobs)

    submit_netstruct_for_all(mac_min_range, mac_max_range, maf_min_range, maf_max_range)
    # submit_netstruct_per_class(mac_min_range, mac_max_range, maf_min_range, maf_max_range, max_number_of_jobs)
