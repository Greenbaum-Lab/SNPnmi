import sys
import time
import random
import os
import glob
import gzip

from os.path import dirname, abspath
root_path = dirname(dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

from utils.common import get_number_of_windows_by_class, build_empty_upper_left_matrix, write_upper_left_matrix_to_file

# python3 merge_windows_to_slices.py maf 0.49 2 2 /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/

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

def merge_windows_to_slices(mac_maf, class_name, num_windows_per_slice, num_slices, classes_folder):
    #  we dont want to use the same seed here, as we will have the same values for all clasees
    # random.seed(10)
    # set paths
    windows_folder = classes_folder + r'windows/'
    slices_folder = classes_folder + r'slices/'
    number_of_windows_per_class_path = classes_folder + 'number_of_windows_per_class.txt'
    windows_class_folder = f'{windows_folder}{mac_maf}_{class_name}/'
    slices_class_folder = f'{slices_folder}{mac_maf}_{class_name}/'

    window_file_regex = f'{windows_folder}{mac_maf}_{class_name}/' + 'count_dist_window_*.tsv.gz'
    window_file_template = f'{windows_folder}{mac_maf}_{class_name}/' + 'count_dist_window_{index}.tsv.gz'
    slice_distances_file_template = slices_class_folder + 'slice_{index}_dist.tsv.gz'
    slice_counts_file_template = slices_class_folder + 'slice_{index}_count.tsv.gz'
    slice_metadata_file_template = slices_class_folder + 'slice_{index}_metadata.txt'
    os.makedirs(slices_class_folder, exist_ok=True)

    # get the expected number of windows
    class2num_windows = get_number_of_windows_by_class(number_of_windows_per_class_path)
    expected_num_of_windows = class2num_windows[str(class_name)]

    # validate that all windows exist
    actual_windows = glob.glob(window_file_regex)
    assert len(actual_windows) == expected_num_of_windows
    max_index = len(actual_windows)
    # Proceessing 100 windows in a slice takes about 1 minute.
    # Output file is about 2MB.
    # we have about 70 classes. Assuming for each we create 100 slices, this will be ~14GB
    for slice_id in range(num_slices):
        print(f'Prepare slice number {slice_id+1} out of {num_slices}')
        windows_to_use = random.sample(range(max_index), num_windows_per_slice)
        windows_to_use.sort()
        print(windows_to_use)
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
classes_folder_local = r"C:\Data\HUJI\hgdp\classes/"
classes_folder = classes_folder_local
mac_maf = 'maf'
class_name = 0.49
num_windows_per_slice = 2
num_slices = 2
merge_windows_to_slices(mac_maf, class_name, num_windows_per_slice, num_slices, classes_folder)

def main(args):
    s = time.time()
    print ('Number of arguments:', len(args), 'arguments.')
    print ('Argument List:', str(args))
    mac_maf = args[0]
    assert mac_maf=='mac' or mac_maf=='maf'
    class_name = args[1]
    num_windows_per_slice = int(args[3])
    assert num_windows_per_slice>=0
    num_slices = int(args[4])
    assert num_slices>0
    classes_folder = args[5]

    print('mac_maf',mac_maf)
    print('class_name',class_name)
    print('num_windows_per_slice',num_windows_per_slice)
    print('num_slices',num_slices)
    print('classes_folder',classes_folder)

    merge_windows_to_slices(mac_maf, class_name, num_windows_per_slice, num_slices, classes_folder)
    print(f'{(time.time()-s)/60} minutes total run time')


if __name__ == "__main__":
   main(sys.argv[1:])