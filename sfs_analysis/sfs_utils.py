import json

import numpy as np
from scipy.interpolate import interp1d

from utils.config import get_sample_sites_file_name


def get_sample_site_list(options, paths_helper):
    sample_sites_path = get_sample_sites_file_name(options.dataset_name)
    with open(paths_helper.data_dir + sample_sites_path, "r") as f:
        sites_list = f.readlines()
    sites_list = [e.replace('\n', '') for e in sites_list]
    return sorted(sites_list)


def get_site2size(paths_helper):
    with open(f"{paths_helper.sfs_dir}summary/site2size.json", "r") as f:
        site2size = json.load(f)
    return site2size

def get_theoretical_sfs(num_of_snps, num_of_genomes):
    res = np.zeros(shape=num_of_genomes)
    for i in range(1, num_of_genomes + 1):
        res[i-1] = 1/i
        if i < num_of_genomes:
            res[i-1] += 1/(2 * num_of_genomes - i)
    res *= (num_of_snps / np.sum(res))
    assert np.abs(np.sum(res) - num_of_snps) < 0.001, f"there are {num_of_snps} snps but sum of res is {res.sum()}, res={res}"
    res[-1] *= 2
    return res

def get_ticks_locations(original_labels, desired_labels):
    p = interp1d(original_labels, np.arange(original_labels.size), fill_value='extrapolate')
    return p(desired_labels)
