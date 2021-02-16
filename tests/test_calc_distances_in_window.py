# This is broken, as calc_distances_in_window no longer returns any value
import sys
from os.path import dirname, abspath
root_path = dirname(dirname(os.path.abspath(__file__)))

from utils.calc_distances_in_window import *


# consider the following 012 files ('../mock/classes/chr1/out.012')
# 4 individuals
# 1 chr
# 4 loci
#  locus: 1,2,3,4
#     id:1,1,2,0,-1
#     id:2,1,2,0,1
#     id:3,0,1,2,2
#     id:4,0,1,2,2

# expected locus stats:
expected_loci_stats = {
    # locus 1: mac 2, maf 0.25(ref)
    1:(2,0.25), 
    # locus 2: mac 2, maf 0.25(non ref)
    2:(2,0.25), 
    # locus 3: mac 4, maf 0.5(ref)
    3:(4,0.5), 
    # locus 4: mac 1, maf 0.16666(non ref)
    4:(1,0.16666666)}

# expected distances per locus
expected_distances_per_locus = {
    # locus 1
    1:{
        1:{
            2:0.25,
            3:0.125,
            4:0.125,
        },
        2:{
            3:0.125,
            4:0.125,
        },
        3:{
            4:0.25,
        }
    },
    # locus 2
    2:{
        1:{
            2:0.25,
            3:0.125,
            4:0.125,
        },
        2:{
            3:0.125,
            4:0.125,
        },
        3:{
            4:0.25,
        }
    },
    # locus 3
    3:{
        1:{
            2:0.5,
            3:0.0,
            4:0.0,
        },
        2:{
            3:0.0,
            4:0.0,
        },
        3:{
            4:0.5,
        }
    },
    # locus 4
    4:{
         1:{
            2:0.0,
            3:0.0,
            4:0.0,
         },
        2:{
            3:0.08333333333333331,
            4:0.08333333333333331,
        },
        3:{
            4:0.16666666666666663,
        }
    }
}

class_name='all'
# PARAMS
# UTILS FOR PARAMS
classes_folder = r"./mock/classes/"
class_012_path_template = classes_folder + r'chr{chr_id}/{mac_maf}_{class_name}.012'
windows_indexes_files_folder = classes_folder + r"windows/indexes/"
windows_indexes_path_template = windows_indexes_files_folder + "windows_indexes_for_class_{class_name}.json"

# if we have less than this which are valid (not -1), site is not included in calc.
min_valid_sites_precentage = 0.1
# if not in range - will raise a warning
mac_maf = 'maf'
min_minor_freq_expected = 0.0
max_minor_freq_expected = 0.5
min_minor_count_expected = -1
max_minor_count_expected = -1
class_name = 'all'

windows_indexes_path = windows_indexes_path_template.format(class_name=class_name)
output_dir = f'{classes_folder}windows/{class_name}/'
for window_index in range(4):
    window_pairwise_counts, window_pairwise_dist = calc_distances_in_window(
                                                    class_012_path_template,
                                                    windows_indexes_path,
                                                    mac_maf,
                                                    class_name,
                                                    window_index,
                                                    window_index+1,
                                                    output_dir,
                                                    min_valid_sites_precentage,
                                                    min_minor_freq_expected,
                                                    max_minor_freq_expected,
                                                    min_minor_count_expected,
                                                    max_minor_count_expected)
    print(window_pairwise_dist)
    print(window_pairwise_counts)
    for zero_based_i1 in range(len(window_pairwise_dist)):
        i1 = 1 + zero_based_i1
        distances = window_pairwise_dist[zero_based_i1]
        for relative_i2 in range(len(distances)):
            i2 = i1 + 1 + relative_i2
            dist = distances[relative_i2]
            # we use 2 chrs, so the expected should be twice what we have
            print('i1', i1, 'i2',  i2, 'dist', dist, 'expected', 2*expected_distances_per_locus[window_index+1][i1][i2])
            assert dist == 2*expected_distances_per_locus[window_index+1][i1][i2]
