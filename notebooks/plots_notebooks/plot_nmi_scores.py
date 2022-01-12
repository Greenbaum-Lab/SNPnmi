#!/usr/bin/python3
import sys
from os.path import dirname, abspath

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.common import args_parser, get_paths_helper, is_class_valid, repr_num
import matplotlib.pyplot as plt
import pandas as pd
import itertools
import numpy as np
from sklearn.metrics import r2_score

options = args_parser()
paths_helper = get_paths_helper(options.dataset_name)
summary_dir = paths_helper.summary_dir
nmi_dir = paths_helper.nmi_dir
nmi_matrix_path = summary_dir + 'nmi_sum_matrix.csv'
nmi_file_template = '{mac_maf}_{val}/{mac_maf}_{val}_all/step_{ns_ss}/{input_type}.txt'

df = pd.read_csv(nmi_matrix_path)
NMI_TYPES = ['AllNodes']  # ['AllNodes', 'Leaves_WithOverlap']
SCORES = ['max']  #  ['max', 'lfk']  # ['max', 'lfk', 'sum']
pairs = list(itertools.product(NMI_TYPES, SCORES))
ALL_SCORES_TYPES = [f'{p[0]}_{p[1]}' for p in pairs]
mac_min_range, mac_max_range = options.mac
maf_min_range, maf_max_range = options.maf
SIZE2COLOR_DICT = {1000: 'b', 5000: 'g', 10000: 'r'}
mac_class_names = np.arange(mac_min_range, mac_max_range + 1) if options.dataset_name != 'arabidopsis' else np.arange(
    mac_min_range, mac_max_range + 1, 2)
maf_class_names = np.arange(maf_min_range, maf_max_range + 1) / 100
line_width = 1.5
scatter_size = 2.5



def _get_scores_from_nmi_file(nmi_file):
    with open(nmi_file) as f:
        lines = f.readlines()
        max_score = float(lines[0].split('\t')[1])
        lfkScore = float(lines[2].split('\t')[1])
        sum_score = float(lines[3].split('\t')[1])
        return max_score, lfkScore, sum_score


for nmi_type, score in pairs:
    score_name = f'{nmi_type}_{score}'

    for mac_maf in ['mac']:  # ['mac', 'maf']:
        all_classes_avg = []
        is_mac = mac_maf == 'mac'
        class_names = mac_class_names if is_mac else maf_class_names
        sizes = [1000, 5000]
        f = plt.figure()
        f.set_figwidth(8)
        f.set_figheight(6)
        ax = f.add_subplot(111)
        for num_of_snp in sizes:
            avg = []
            std = []
            min_range = mac_min_range if is_mac else maf_min_range
            max_range = mac_max_range if is_mac else maf_max_range
            for val in range(min_range, max_range + 1):
                if not is_class_valid(options, mac_maf, val):
                    continue
                # in maf we take 0.x
                if not is_mac:
                    val = f'{val * 1.0 / 100}'
                if num_of_snp == sizes[0]:
                    all_class_file = nmi_dir + nmi_file_template.format(mac_maf=mac_maf, val=val, ns_ss=options.ns_ss,
                                                                           input_type=nmi_type)
                    max_score, lfk_score, sum_score = _get_scores_from_nmi_file(all_class_file)
                    all_classes_avg.append(max_score)
                class_name = f'ss_{options.ns_ss}_{mac_maf}_{val}_{num_of_snp}'
                class_values = df[df.Class == class_name]
                if len(class_values) == 0:
                    continue
                n = int(class_values['num_of_trees'])
                avg.append(float(class_values[f'{score_name}_avg']))
                std.append(float(class_values[f'{score_name}_std']))
            avg = np.array(avg)
            std = np.array(std)
            plt.plot(class_names, avg, color=SIZE2COLOR_DICT[num_of_snp], label=num_of_snp, linewidth=line_width)
            plt.fill_between(class_names, y1=avg - std, y2=avg + std, alpha=0.3, color=SIZE2COLOR_DICT[num_of_snp])
            plt.scatter(class_names, avg, color=SIZE2COLOR_DICT[num_of_snp], s=scatter_size)

        plt.scatter(class_names, all_classes_avg, label='Full class', color='tab:orange', s=scatter_size)
        z = np.polyfit(class_names, all_classes_avg, 3)
        p = np.poly1d(z)
        plt.plot(class_names, p(class_names), color='tab:orange', linestyle='--', linewidth=line_width)
        y_hat = np.poly1d(z)(class_names)
        e = [f'{repr_num(z[i])}' if i == 0 or z[i] < 0 else f'+{repr_num(z[i])}' for i in range(len(z))]
        equation = [f'{e[i]} x^{len(e) - (i+1)}' if len(e) - (i+1) > 1 else f'{e[i]}' if len(e) - (i+1) == 0 else f'{e[i]} x' for i in range(len(e))]
        text = f"$y={''.join(equation)}$\n$R^2 = {repr_num(r2_score(all_classes_avg, y_hat))}$"
        plt.gca().text(0.01, 0.99, text, transform=plt.gca().transAxes,
                       fontsize=10, verticalalignment='top')
        plt.plot(0.05, 0.95, transform=plt.gca().transAxes, color='none')
        plt.xlabel(f"{mac_maf}", fontsize=16)
        plt.xticks(fontsize=10)
        plt.yticks(fontsize=10)
        plt.ylabel("NMI score", fontsize=16)
        legend = plt.legend(title="Num of SNPs", loc='best', fontsize=12)
        plt.setp(legend.get_title(), fontsize=12)
        plt.title(f'{options.dataset_name} - {nmi_type} - {mac_maf}', fontsize=16)
        plt.savefig(f'{summary_dir}fix_size_nmi_scores/{mac_maf}_{score_name}.svg')
        plt.clf()
