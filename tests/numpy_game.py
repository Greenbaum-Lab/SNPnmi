import gzip
import json
import os
import subprocess
import time
from random import sample
import matplotlib.pyplot as plt

import numpy as np


def file012_to_numpy(input_file_path, raw_file=None):
    if raw_file is None:
        with gzip.open(input_file_path, 'rb') as f:
            raw_file = f.read().decode()
    split_individuals = raw_file.split('\n')
    if split_individuals[-1] == '':  # we throw empty line at the end of the file
        split_individuals = split_individuals[:-1]
    split_sites = [individual.split(' ') for individual in split_individuals]
    new_mat = np.zeros(shape=(929, 929))
    for i in range(len(split_sites)):
        for j in range(len(split_sites[i])):
            new_mat[i, i + j] = float(split_sites[i][j])
    for i in range(929):
        for j in range(i):
            new_mat[i, j] = new_mat[j, i]
    return new_mat


def compare_amir_similarities():
    amir_dir_path = "/vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/distances/"
    files = [f for f in os.listdir(amir_dir_path) if "_all_norm_dist.tsv.gz" in f]
    for f in files:
        nump = file012_to_numpy(amir_dir_path + f)
        class_name = f.replace("_all_norm_dist.tsv.gz", "")
        print(f"class_name: {class_name}")
        print(f"type: {type(nump)}")
        print(f"shape: {nump.shape}")
        print(f"min,max: {nump.min(), nump.max()}")
        exit(0)


def test_subprocess():
    a = os.popen('top -bi -n 1').readlines()
    c = [i for i in a if 'vcftools' in i]
    print(len(c))

def log_plots():
    plt.plot([1002,105,13,2,1,8,6,4])
    plt.yscale('log')
    plt.show()

def legend_loc_check():
    x = [1, 2]
    plt.gca().text(0.05, 0.95, 'some text', transform=plt.gca().transAxes, verticalalignment='top')
    plt.plot(x, x, label='plot name')
    plt.plot(0.05, 0.95, transform=plt.gca().transAxes, color='none')
    plt.legend(loc='best')
    plt.show()

legend_loc_check()