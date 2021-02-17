# python3 merge_windows_to_slices.py maf 0.49 2 2 True

import sys
import time
import random
import os
import glob
import gzip

from os.path import dirname, abspath
root_path = dirname(dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

from utils.common import get_number_of_windows_by_class, build_empty_upper_left_matrix, write_upper_left_matrix_to_file, str2bool, get_paths_helper

def write_metadata_to_file(windows_to_use, file):
    with open(file,'w') as f:
        windows_s = ','.join([str(i) for i in windows_to_use])
        f.write(f'windows used:{windows_s}\n')


def calc_distances_based_on_files(files):
    # use the first file to understand the number of individuals
    with gzip.open(files[0], 'rb') as f:
        num_ind = len(f.readline().split()) + 1
    dists = build_empty_upper_left_matrix(num_ind, 0.0)
    counts = build_empty_upper_left_matrix(num_ind, 0)

    # sum it up!
    for file in files:
        with gzip.open(files[0], 'rb') as f:
            line = f.readline().decode()
            i = -1
            while line:
                i += 1
                parts = line.replace('\n','').split()
                assert len(parts) == num_ind - 1 - i
                for j, count_dist in enumerate(parts):
                    count, dist = count_dist.split(';', 1)
                    counts[i][j] += int(count)
                    dists[i][j] += float(dist)
                line = f.readline().decode()
            # minus 1 as we only have i-j (wihtou i-i) minus another one as the count is zero based 
            assert i == num_ind - 1 - 1
    # now return the true distance - that is, the sum of distances divided by sum of counts
    for i in range(len(dists)):
        for j in range(len(dists[i])):
            dists[i][j] = dists[i][j] / counts[i][j]
    return dists, counts

def merge_windows_to_slices(mac_maf, class_name, num_windows_per_slice, given_num_slices, is_random):
    if is_random:
        print('merge to random slices')
    else:
        print('merge to sequential slices')
    #  we dont want to use the same seed here, as we will have the same values for all clasees
    # random.seed(10)
    paths_helper = get_paths_helper()

    windows_class_folder = f'{paths_helper.windows_folder}{mac_maf}_{class_name}/'
    slices_class_folder = f'{paths_helper.random_slices_folder if is_random else paths_helper.slices_folder}{mac_maf}_{class_name}/' 

    # used to assert number of windows
    window_file_regex = f'{paths_helper.windows_folder}{mac_maf}_{class_name}/' + 'count_dist_window_*.tsv.gz'
    window_file_template = f'{paths_helper.windows_folder}{mac_maf}_{class_name}/' + 'count_dist_window_{index}.tsv.gz'

    slice_distances_file_template = slices_class_folder + 'slice_{index}_dist.tsv.gz'
    slice_counts_file_template = slices_class_folder + 'slice_{index}_count.tsv.gz'
    slice_metadata_file_template = slices_class_folder + 'slice_{index}_metadata.txt'
    os.makedirs(slices_class_folder, exist_ok=True)

    # get the expected number of windows
    class2num_windows = get_number_of_windows_by_class(paths_helper.number_of_windows_per_class_path)
    expected_num_of_windows = class2num_windows[str(class_name)]

    # validate that all windows exist
    actual_windows = glob.glob(window_file_regex)
    assert len(actual_windows) == expected_num_of_windows, f'{len(actual_windows)} != {expected_num_of_windows}'
    max_index = len(actual_windows)

    # Proceessing 100 windows in a slice takes about 1 minute.
    # Output file is about 2MB.
    # we have about 70 classes. Assuming for each we create 100 slices, this will be ~14GB
    if is_random:
        num_slices = given_num_slices
    else:
        num_slices = int(max_index/num_windows_per_slice) + bool(max_index%num_windows_per_slice)
    for slice_id in range(num_slices):
        print(f'Prepare slice number {slice_id+1} out of {num_slices}')
        if is_random:
            windows_to_use = random.sample(range(max_index), num_windows_per_slice)
            windows_to_use.sort()
        else:
            slice_min_windos_index = num_windows_per_slice*slice_id
            slice_max_windos_index = min(max_index, num_windows_per_slice*(slice_id+1))
            print(f'Windows indexes [{slice_min_windos_index}, {slice_max_windos_index})')
            windows_to_use = range(slice_min_windos_index, slice_max_windos_index)

        slice_metadata_file = slice_metadata_file_template.format(index=slice_id)
        write_metadata_to_file(windows_to_use, slice_metadata_file)
        print(f'slice_metadata_file : {slice_metadata_file}')

        distances, counts = calc_distances_based_on_files([window_file_template.format(index=i) for i in windows_to_use])
        slice_distances_file = slice_distances_file_template.format(index=slice_id)
        slice_counts_file = slice_counts_file_template.format(index=slice_id)
        write_upper_left_matrix_to_file(slice_distances_file, distances)
        print(f'slice_distances_file : {slice_distances_file}')
        write_upper_left_matrix_to_file(slice_counts_file, counts)
        print(f'slice_counts_file : {slice_counts_file}')

# params
# mac_maf = 'maf'
# class_name = 0.49
# num_windows_per_slice = 5
# num_slices = 2
# is_random = False
# merge_windows_to_slices(mac_maf, class_name, num_windows_per_slice, num_slices, is_random)

def main(args):
    s = time.time()
    print ('Number of arguments:', len(args), 'arguments.')
    print ('Argument List:', str(args))
    mac_maf = args[0]
    assert mac_maf=='mac' or mac_maf=='maf'
    class_name = args[1]
    num_windows_per_slice = int(args[2])
    assert num_windows_per_slice>=0
    num_slices = int(args[3])
    assert num_slices>0
    is_random = str2bool(args[4])

    print('mac_maf',mac_maf)
    print('class_name',class_name)
    print('num_windows_per_slice',num_windows_per_slice)
    print('num_slices',num_slices)
    print('is_random',is_random)

    merge_windows_to_slices(mac_maf, class_name, num_windows_per_slice, num_slices, is_random)

    print(f'{(time.time()-s)/60} minutes total run time')


if __name__ == "__main__":
   main(sys.argv[1:])