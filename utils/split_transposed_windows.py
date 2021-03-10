# python3 split_transposed_windows.py 2 -1 0 72
import sys
import time
import os
import json 
import pandas as pd
import random
import gzip

from os.path import dirname, abspath
root_path = dirname(dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

from utils.common import normalize_distances, get_number_of_windows_by_class, build_empty_upper_left_matrix, write_upper_left_matrix_to_file, str2bool, get_paths_helper
from utils.config import get_num_chrs, get_num_individuals

random.seed(a='42', version=2)

def append_to_dic(dic, key, value):
    if not key in dic.keys():
        dic[key] = []
    dic[key].append(value)

def split_file(input_file, output_file_name_template, number_of_splits, split_index_2_original_line_numbers_file, metadata_for_dic):
        split_index_2_original_line_numbers = dict()
        print(f'Open {number_of_splits} output_files')
        output_files = [gzip.open(output_file_name_template.format(split_id=i), 'ab') for i in range(number_of_splits)]
        #output_files = [open(output_file_name_template.format(split_id=i), 'a') for i in range(number_of_splits)]
        print(f'Done')

        with gzip.open(input_file, 'rb') as f:
        #with open(input_file, 'r') as f:
            l =  f.readline()
            i = 0
            while l:
                if i%100 == 0:
                    print(f'Fone {i} in file {input_file}')
                split_index = random.randint(0, number_of_splits-1)
                append_to_dic(split_index_2_original_line_numbers, split_index, f'{metadata_for_dic};{i}')
                output_file = output_files[split_index]
                output_file.write(l)
                i += 1
                l = f.readline()
        print(f'Close {number_of_splits} output_files')
        for f in output_files:
            f.close()
        print(f'Done')
        with open(split_index_2_original_line_numbers_file, "w" ) as f:
            json.dump(split_index_2_original_line_numbers, f )

def split_transposed_windows(mac_maf, class_name, window_id, number_of_splits):
    #  we dont want to use the same seed here, as we will have the same values for all clasees
    paths_helper = get_paths_helper()

    # /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/windows/mac_2/transposed/
    window_transposed_folder = paths_helper.windows_folder + f'{mac_maf}_{class_name}/transposed/'

    # input - window_0_transposed.tsv.gz
    input_window_transposed = window_transposed_folder + f'window_{window_id}_transposed.012.tsv.gz'

    # output splits
    output_splits_transposed_folder = window_transposed_folder + f'window_{window_id}/'
    output_splits_transposed_template = output_splits_transposed_folder + 'split_{split_id}_transposed.012.tsv.gz'
    
    # output indexes
    output_indexes_file = paths_helper.windows_indexes_template.format(class_name=f'{class_name}_window_{window_id}_split')

    print(input_window_transposed)
    print(output_splits_transposed_template)
    print(output_indexes_file)

    # make sure output folders exists
    os.makedirs(paths_helper.windows_indexes_folder, exist_ok=True)
    os.makedirs(output_splits_transposed_folder, exist_ok=True)

    assert os.path.exists(input_window_transposed)
    split_file(input_window_transposed, output_splits_transposed_template, number_of_splits, output_indexes_file, metadata_for_dic = '')

def main(args):
    s = time.time()
    print ('Number of arguments:', len(args), 'arguments.')
    print ('Argument List:', str(args))
    mac = args[0]
    maf = args[1]
    window_id = args[2]
    numbe_of_splits = args[3]

    print('mac',mac)
    print('maf',maf)
    print('window_id',window_id)
    print('numbe_of_splits',numbe_of_splits)

    if int(mac) > 0:
        print(f'Will split window {window_id} for mac {mac}')
        split_transposed_windows('mac', mac, window_id, numbe_of_splits)
    if float(maf) > 0:
        print(f'Will split window {window_id} for maf {maf}')
        split_transposed_windows('maf', maf, window_id, numbe_of_splits)

    print(f'{(time.time()-s)/60} minutes total run time')

# params
# mac = -1
# maf = 0.49
# window_id = 0
# numbe_of_splits = 10
# main([mac, maf, window_id, numbe_of_splits])

if __name__ == "__main__":
    main(sys.argv[1:])
