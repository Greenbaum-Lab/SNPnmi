#!/usr/bin/env python
# coding: utf-8

# In[22]:


# %run notebooks/plots_notebooks/number_of_snp_per_class.ipynb

from utils.common import is_cluster
import matplotlib.pyplot as plt
import json
import numpy as np
if is_cluster():
    summary_dir = r"/vol/sci/bio/data/gil.greenbaum/shahar.mazie/vcf/hgdp/classes/summary/"
    windows_dir = r"/vol/sci/bio/data/gil.greenbaum/shahar.mazie/vcf/hgdp/classes/windows/"
else:
    summary_dir = r"/home/lab2/shahar/cluster_dirs/vcf/hgdp/classes/summary/"
    windows_dir = r"/home/lab2/shahar/cluster_dirs/vcf/hgdp/classes/windows/"

window_to_snps_path =  windows_dir + 'number_of_windows_per_class.txt'
window_size_path = windows_dir + 'window_size.txt'
mac_min_range = 2
mac_max_range = 70
maf_min_range = 1
maf_max_range = 49
mac_class_names = np.arange(mac_min_range, mac_max_range+1)
maf_class_names = np.arange(maf_min_range, maf_max_range+1) / 100


# In[23]:


with open(window_to_snps_path) as f:
    windows2count = json.load(f)
with open(window_size_path, 'r') as f:
    window_size = int(f.readline())
num_of_snps = []
for mac_maf in ['mac', 'maf']:
    is_mac = mac_maf == 'mac'
    min_range = mac_min_range if is_mac else maf_min_range
    max_range = mac_max_range if is_mac else maf_max_range
    for val in range(min_range, max_range+1):
        # in maf we take 0.x
        if not is_mac:
            val = f'{val * 1.0/100}'
        class_name = f'{mac_maf}_{val}'
        num_of_snps.append(int(windows2count[class_name]) * window_size)
    class_names = mac_class_names if is_mac else maf_class_names
    plt.plot(class_names, num_of_snps)
    plt.yscale('log')
    plt.xlabel(f"{mac_maf}")
    plt.title('Num of SNPs per class')
    plt.savefig(f'{summary_dir}num_of_snps/{mac_maf}.svg')
    plt.clf()
    num_of_snps = []

