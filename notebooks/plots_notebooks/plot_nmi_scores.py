#!/usr/bin/env python
# coding: utf-8

# In[1]:


from utils.common import is_cluster
import matplotlib.pyplot as plt
import pandas as pd
import itertools
import numpy as np
dataset_name = 'hgdp'
if is_cluster():
    summary_dir = f"/vol/sci/bio/data/gil.greenbaum/shahar.mazie/vcf/{dataset_name}/classes/summary/"
else:
    summary_dir = f"/home/lab2/shahar/cluster_dirs/vcf/{dataset_name}/classes/summary/"
if is_cluster():
    nmi_folder = f"/vol/sci/bio/data/gil.greenbaum/shahar.mazie/vcf/{dataset_name}/classes/nmi/"
else:
    nmi_folder = f"/home/lab2/shahar/cluster_dirs/vcf/{dataset_name}/classes/nmi/"
nmi_matrix_path =  summary_dir + 'nmi_sum_matrix.csv'
nmi_file_template = '{mac_maf}_{val}/{mac_maf}_{val}_all/step_{ns_ss}/{input_type}.txt'
df = pd.read_csv(nmi_matrix_path)
ns_ss = 0.01
NMI_TYPES = ['AllNodes', 'Leaves_WithOverlap']
SCORES = ['max']   # ['max', 'lfk', 'sum']
pairs = list(itertools.product(NMI_TYPES, SCORES))
ALL_SCORES_TYPES = [f'{p[0]}_{p[1]}' for p in pairs]
mac_min_range = 2
mac_max_range = 70
maf_min_range = 1
maf_max_range = 49
SIZE2COLOR_DICT = {1000: 'b', 5000:'g', 10000: 'r'}
mac_class_names = np.arange(mac_min_range, mac_max_range+1)
maf_class_names = np.arange(maf_min_range, maf_max_range+1) / 100


def _get_scores_from_nmi_file(nmi_file):
    with open(nmi_file) as f:
        lines = f.readlines()
        max_score = float(lines[0].split('\t')[1])
        lfkScore = float(lines[2].split('\t')[1])
        sum_score = float(lines[3].split('\t')[1])
        return max_score, lfkScore, sum_score


# In[3]:


for nmi_type, score in pairs:
    score_name = f'{nmi_type}_{score}'

    for mac_maf in ['mac', 'maf']:
        all_classes_avg = []
        is_mac = mac_maf == 'mac'
        class_names = mac_class_names if is_mac else maf_class_names
        sizes = [1000, 5000]
        f = plt.figure()
        f.set_figwidth(15)
        f.set_figheight(15)
        ax = f.add_subplot(111)
        for num_of_snp in sizes:
            avg = []
            std = []
            min_range = mac_min_range if is_mac else maf_min_range
            max_range = mac_max_range if is_mac else maf_max_range
            for val in range(min_range, max_range+1):
                if dataset_name == 'arabidopsis' and is_mac and val % 2 == 1:
                    continue
                # in maf we take 0.x
                if not is_mac:
                    val = f'{val * 1.0/100}'
                if num_of_snp == sizes[0]:
                    all_class_file = nmi_folder + nmi_file_template.format(mac_maf=mac_maf, val=val, ns_ss=ns_ss, input_type=nmi_type)
                    max_score, lfk_score, sum_score = _get_scores_from_nmi_file(all_class_file)
                    all_classes_avg.append(max_score)
                class_name = f'ss_{ns_ss}_{mac_maf}_{val}_{num_of_snp}'
                class_values = df[df.Class == class_name]
                if len(class_values) == 0:
                    continue
                n = int(class_values['num_of_trees'])
                avg.append(float(class_values[f'{score_name}_avg']))
                std.append(float(class_values[f'{score_name}_std']))
            avg = np.array(avg)
            std = np.array(std)
            plt.plot(class_names, avg, color=SIZE2COLOR_DICT[num_of_snp], label=num_of_snp)
            plt.scatter(class_names, avg, color=SIZE2COLOR_DICT[num_of_snp])
            plt.fill_between(class_names, y1=avg - std, y2=avg + std, alpha=0.3, color=SIZE2COLOR_DICT[num_of_snp])

        plt.scatter(class_names, all_classes_avg)
        plt.xlabel(f"{mac_maf}")
        plt.legend(title="Num of SNPs")
        plt.title(f'{score_name}')
        plt.savefig(f'{summary_dir}fix_size_nmi_scores/{mac_maf}_{score_name}.svg')
        plt.clf()
    


# In[ ]:




