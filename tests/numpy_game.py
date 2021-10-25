import gzip
import json
import os
from random import sample

import numpy as np


def file012_to_numpy(input_file_path, raw_file=None):
    if raw_file is None:
        with open(input_file_path, 'rb') as f:
            raw_file = f.read()#.decode()
    split_individuals = raw_file.split('\n')
    if split_individuals[-1] == '':  # we throw empty line at the end of the file
        split_individuals = split_individuals[:-1]
    split_sites = [individual.split('\t') for individual in split_individuals]
    arr = np.array(split_sites, dtype=np.int8)
    if np.any(arr[:, 0] > 2):
        arr = arr[:, 1:]  # First column is individual number.
    return arr


def compare_amir_similarities():
    amir_dir_path = "/vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/distances/"
    files = [f for f in os.listdir(amir_dir_path) if "_all_norm_dist.tsv.gz" in f]
    for f in files:
        nump = file012_to_numpy(amir_dir_path + f)
        class_name = f.replace("_all_norm_dist.tsv.gz", "")
        print(f"class_name: {class_name}")
        print(f"type: {type(nump)}")
        print(f"min,max: {nump.min(), nump.max()}")
        exit(0)

compare_amir_similarities()