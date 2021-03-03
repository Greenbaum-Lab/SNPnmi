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

def random_split_class_from_chr_to_windows(chr_id, chr_class_file, number_of_windows, windows_indexes_file, window_transposed_files, batch_size=2000):
    # load file to memory
    with open(chr_class_file,'r') as f:
        num_columns = len(f.readline().split('\t'))
    print(f'Number of sites: {num_columns}')
    window_index_2_site_index = dict()
    with open(chr_class_file,'r') as f:
        # the first column is the index!
        num_columns = len(f.readline().split('\t'))
    # the first column is the index!
    min_col_index_in_batch = 1
    max_col_index_in_batch = min(num_columns, batch_size)
    batch_index = 1
    num_batches = int(num_columns/batch_size) + 1
    print(f'Will have {num_batches}, each of max size {batch_size}')

    while min_col_index_in_batch < max_col_index_in_batch:
        print(f'Process [{min_col_index_in_batch}, {max_col_index_in_batch}) in batch {batch_index}/{num_batches}')
        usecols = range(min_col_index_in_batch, max_col_index_in_batch)
        names = [str(i) for i in usecols]
        batch_df = pd.read_csv(chr_class_file, sep='\t', usecols=usecols, index_col=False, names=names)
        # validate we dont have the index column
        assert batch_df.iloc[:,0].max() <= 2

        process_batch_df(batch_df, batch_index, num_batches, number_of_windows, window_index_2_site_index, chr_id, window_transposed_files)
        
        min_col_index_in_batch = min(num_columns, max_col_index_in_batch)
        max_col_index_in_batch = min(num_columns, max_col_index_in_batch + batch_size)
        batch_index += 1    

    validate_windows_indexes(window_index_2_site_index, chr_id, num_columns-1, number_of_windows)

    with open(windows_indexes_file, "w" ) as f:
            json.dump(window_index_2_site_index, f )

def process_batch_df(batch_df, batch_index, num_batches, number_of_windows, window_index_2_site_index, chr_id, window_transposed_files):
    i = 0
    for (columnName, columnData) in batch_df.iteritems():
        i += 1
        if i%200 == 0:
            msg = f'\t\tChr{chr_id}: done {i}/{len(batch_df.columns)} sites in batch {batch_index}/{num_batches}'
            print(msg)
            with open('./log.txt', 'a') as f:
                f.write(msg + '\n')
        window_index = random.randint(0, number_of_windows-1)
        # store the index in the file mapping windows to indexes used
        append_to_dic(window_index_2_site_index, window_index, f'{chr_id};{columnName}')
        # append the column as a row to the right window file
        window_transposed_file = window_transposed_files[window_index]
        s = '\t'.join([str(i) for i in columnData.values]) + '\n'
        window_transposed_file.write(s.encode())

def validate_windows_indexes(windows_indexes, chr_id, expected_num_indexes, expected_num_windows):
    all_indexes = []
    for k in windows_indexes.keys():
        for c_i in windows_indexes[k]:
            c,i = c_i.split(';',2)
            assert c==str(chr_id)
            ii = int(i)
            assert not ii in all_indexes
            all_indexes.append(ii)
    assert len(all_indexes) == expected_num_indexes
    assert len(windows_indexes.keys()) <= expected_num_windows
    assert len(windows_indexes.keys()) > expected_num_windows/2
    print(f'validate_windows_indexes passed: {expected_num_indexes} indexes writen to {len(windows_indexes.keys())} windows')

def generate_windows_and_indexes_files(mac_maf, class_name):
    #  we dont want to use the same seed here, as we will have the same values for all clasees
    paths_helper = get_paths_helper()

    # inputs are /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/chr21/mac_2.012
    input_per_chr_template = paths_helper.classes_folder + 'chr{chr_id}/' + f'{mac_maf}_{class_name}.012'
    
    # outputs are 
    # indexes: /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/windows/indexes/windows_indexes_for_class_2.json
    windows_indexes_file_template = paths_helper.windows_indexes_template.format(class_name=f'{class_name}_chr'+'{chr_id}')
    
    # transposed windows: 
    # /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/windows/mac_2/transposed/window_0_transposed.tsv.gz
    window_transposed_template = paths_helper.windows_folder + f'{mac_maf}_{class_name}/transposed/window_' + '{window_id}_transposed.012.tsv.gz'

    print(input_per_chr_template)
    print(windows_indexes_file_template)
    print(window_transposed_template)

    # make sure output folders exists
    os.makedirs(paths_helper.windows_indexes_folder, exist_ok=True)
    os.makedirs('/'.join(window_transposed_template.split('/')[:-1]), exist_ok=True)
    number_of_windows = 7303 # get_number_of_windows_by_class()[str(class_name)]
    # TODO - this is a brute force to solve the problem I have with mac2
    # Forcing 7303 instead of 73031 windows

    print(number_of_windows)

    num_chrs = get_num_chrs()
    print(num_chrs)

    print(f'Open {number_of_windows} window_transposed_files')
    window_transposed_files = [gzip.open(window_transposed_template.format(window_id=i), 'ab') for i in range(number_of_windows)]
    print(f'Done')

    # go over chrs
    for chr_id in range(1,num_chrs+1):
        s = time.time()
        print(f'{s} Start process chr {chr_id}')
        chr_class_file = input_per_chr_template.format(chr_id=chr_id)
        windows_indexes_file = windows_indexes_file_template.format(chr_id=chr_id)
        random_split_class_from_chr_to_windows(chr_id, chr_class_file, number_of_windows, windows_indexes_file, window_transposed_files)
        print('TODO remove')
        break

    print(f'Close {number_of_windows} window_transposed_files')
    for f in window_transposed_files:
        f.close()
    print(f'Done')

def transpose_and_gzip(window_transposed_file, window_not_transposed_file):
    # transpose and output to file
    with gzip.open(window_transposed_file,'rb') as f:
            num_columns = len(f.readline().decode().split('\t'))
    names = [f'{i+1}' for i in range(num_columns)]
    df1 = pd.read_csv(window_transposed_file, compression='gzip', sep='\t', names=names) 
    df1.transpose()
    df1.transpose().to_csv(window_not_transposed_file, index=True, header=False, compression='gzip')


def main(args):
    s = time.time()
    print ('Number of arguments:', len(args), 'arguments.')
    print ('Argument List:', str(args))
    mac = args[0]
    maf = args[1]

    print('mac',mac)
    print('maf',maf)

    if int(mac) > 0:
        print(f'Will generate windows for mac {mac}')
        generate_windows_and_indexes_files('mac', mac)
    if float(maf) > 0:
        print(f'Will generate windows for maf {maf}')
        generate_windows_and_indexes_files('maf', maf)

    print(f'{(time.time()-s)/60} minutes total run time')

# params
# mac=-1
# maf=0.49
# main([mac, maf])

if __name__ == "__main__":
    # go over chrs one by one
    # load the mac_2.012 file to pandas
    # go over the COLUMNS
    # each column, pick a number in 0-73031
    # append the column AS A ROW to the windows_transposed file
    # also make sure to write the index
    # after all chrs are done, go over the 73K transposed windows, and transpose them
    main(sys.argv[1:])