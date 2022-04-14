#!/usr/bin/env python
import json
import os
import sys
from os.path import dirname, abspath, basename
import matplotlib.pyplot as plt
import numpy as np

root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)


def plot_per_class(options, mac_maf, values, title, output):
    mac_class_names = np.arange(options.mac[0], options.mac[1] + 1) if options.dataset_name != 'arabidopsis' else \
        np.arange(options.mac[0], options.mac[1] + 1, 2)
    maf_class_names = np.arange(options.maf[0], options.maf[1] + 1) / 100
    class_names = mac_class_names if mac_maf == ' maf' else maf_class_names

    plt.plot(class_names, values)
    plt.yscale('log')
    plt.xlabel(f"{mac_maf}", fontsize=16)
    plt.xticks(fontsize=10)
    plt.yticks(fontsize=10)
    plt.title(title, fontsize=18)
    plt.savefig(output)
    plt.clf()
