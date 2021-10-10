import gzip
import json
import os
from random import sample

import numpy as np
from steps.s3_split_to_windows import split_chr_class_to_windows
from utils.filelock import FileLock

directory_to_compare = "/home/lab2/shahar/cluster_dirs/vcf/hgdp_test/classes/windows/mac_3/chr21/"

def what():
    all_files = os.listdir(directory_to_compare)
    np_files = [file[:-3] for file in all_files if 'npy' in file]
    vcf_files = [file[:-6] for file in all_files if 'vcf.gz' in file]
    assert set(np_files) == set(vcf_files)
    for file in np_files:
        with open(directory_to_compare + file + 'npy', 'rb') as np_f:
            matrix = np.load(np_f)
        with gzip.open(directory_to_compare + file + 'vcf.gz', 'r') as gz_f:
            vcf012 = gz_f.read().decode()
        new_mat = split_chr_class_to_windows.file012_to_numpy(None, vcf012)
        new_file = split_chr_class_to_windows.numpy_to_file012(None, matrix)
        assert new_file == vcf012
        assert np.all(new_mat == matrix)
        print(f"Done with file {file}")

def game():
    file_type='count'
    with open(f"/home/lab2/shahar/cluster_dirs/vcf/hgdp/classes/similarity/mac_2_all_{file_type}.npy", 'rb') as f:
        old_np = np.load(f)
    print()

game()