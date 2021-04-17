# python3 transpose_windows.py 2 -1 0 0 72
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

from utils.common import get_paths_helper

random.seed(a='42', version=2)

is_gzip = True

def transpose_and_gzip(window_transposed_file, window_not_transposed_file):
    # transpose and output to file
    with gzip.open(window_transposed_file,'rb') if is_gzip else open(window_transposed_file,'r') as f:
            num_columns = len(f.readline().decode().split('\t')) if is_gzip else len(f.readline().split('\t'))
    names = [f'{i+1}' for i in range(num_columns)]
    df1 = pd.read_csv(window_transposed_file, compression='gzip' if is_gzip else None, sep='\t', names=names) 
    df1.transpose().to_csv(window_not_transposed_file, index=True, header=False, sep='\t', compression='gzip')

def transpose_windows(mac_maf, class_name, window_id, first_index_to_use, expected_number_of_files):
    #  we dont want to use the same seed here, as we will have the same values for all clasees
    paths_helper = get_paths_helper()

    # /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/windows/mac_2/transposed/
    windows_012_folder = paths_helper.windows_folder + f'{mac_maf}_{class_name}/'
    output_windows_012_file_template = windows_012_folder +'window_{window_id}.012.tsv.gz'

    windows_transposed_folder = windows_012_folder + '/transposed/'

    # input splits
    input_splits_transposed_folder = windows_transposed_folder + f'window_{window_id}/'
    
    # output mapping
    # we convert each part to a 012 file - the mapping is kept here
    output_mapping_file = input_splits_transposed_folder + 'split_file_to_012_file.json'

    print(windows_012_folder)
    print(windows_transposed_folder)
    print(input_splits_transposed_folder)
    print(output_mapping_file)

    output_index_counter = first_index_to_use - 1
    input_2_output = dict()
    files = [name for name in os.listdir(input_splits_transposed_folder) if name.endswith('_transposed.012.tsv.gz')]
    assert len(files) == expected_number_of_files, f'expecting {expected_number_of_files}, found {len(files)}'
    for split_file_name in files:
        output_index_counter += 1
        window_transposed_file = input_splits_transposed_folder + split_file_name
        window_not_transposed_file = output_windows_012_file_template.format(window_id=output_index_counter)
        transpose_and_gzip(window_transposed_file, window_not_transposed_file)
        # only take file names
        input_2_output[window_transposed_file.split('/')[-1]] = window_not_transposed_file.split('/')[-1]

    with open(output_mapping_file, "w" ) as f:
        json.dump(input_2_output, f )

def main(args):
    s = time.time()
    print ('Number of arguments:', len(args), 'arguments.')
    print ('Argument List:', str(args))
    mac = args[0]
    maf = args[1]
    window_id = args[2]
    first_index_to_use = int(args[3])
    expected_number_of_files = int(args[4])
    print('mac',mac)
    print('maf',maf)
    print('window_id',window_id)
    print('first_index_to_use',first_index_to_use)
    print('expected_number_of_files',expected_number_of_files)

    if int(mac) > 0:
        print(f'Will transpose splits of window {window_id} for mac {mac}')
        transpose_windows('mac', mac, window_id, first_index_to_use, expected_number_of_files)
    if float(maf) > 0:
        print(f'Will transpose splits of window {window_id} for maf {maf}')
        transpose_windows('maf', maf, window_id, first_index_to_use, expected_number_of_files)

    print(f'{(time.time()-s)/60} minutes total run time')

# #params
# mac = -1
# maf = 0.49
# window_id = 1
# first_index_to_use = 3
# expected_number_of_files = 3
# main([mac, maf, window_id, first_index_to_use, expected_number_of_files])

if __name__ == "__main__":
    main(sys.argv[1:])
