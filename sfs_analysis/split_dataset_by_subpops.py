import json
import os
import shutil
import subprocess
from io import StringIO
from os.path import dirname, abspath
import sys

root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)
from utils import config
sys.path.insert(0, f'{config.get_config(config.CONFIG_NAME_PATHS)["venv_path"]}lib/python3.7/site-packages')


from utils.loader import Timer
from scipy.ndimage.filters import gaussian_filter
from utils.config import get_sample_sites_file_name, get_indlist_file_name, get_dataset_metadata_files_names
from utils.common import args_parser, get_paths_helper
from sfs_analysis.sfs_utils import get_sample_site_list

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def plot_subpopulations_size_histogram(options, paths_helper):
    vcf_dir = paths_helper.data_dir
    sample_sites_path = get_sample_sites_file_name(options.dataset_name)
    indlist_path = get_indlist_file_name(options.dataset_name)
    sites_list = get_sample_site_list(options, paths_helper)
    with open(vcf_dir + indlist_path, "r") as f:
        indlist = f.readlines()
    indlist = [e.replace('\n', '') for e in indlist]
    site_hist = {e: 0 for e in sites_list}
    for site in indlist:
        site_hist[site] += 1
    assert sum(site_hist.values()) == len(indlist)
    freq_hist = np.zeros(max(site_hist.values()))
    for count in site_hist.values():
        freq_hist[count - 1] += 1
    plt.plot(np.arange(freq_hist.size) + 1, gaussian_filter(freq_hist, 1))
    plt.scatter(np.arange(freq_hist.size) + 1, gaussian_filter(freq_hist, 1), s=10)
    plt.title(f"Histogram of subpopulation sizes in {options.dataset_name}")
    plt.xlabel("Number of individuals")
    plt.ylabel("Number of subpopulations")
    plt.savefig(f"{paths_helper.sfs_dir}subpopulations_histogram.svg")


def hgdp_create_site2samples(options, paths_helper):
    meta_data_file_path = paths_helper.data_dir + 'hgdp_wgs.20190516.metadata.txt'
    with open(meta_data_file_path, "r") as f:
        meta_data = f.read()
    meta_df = pd.read_csv(StringIO(meta_data), sep='\t')
    sample_sites = get_sample_site_list(options, paths_helper)
    site2samples = {}
    for site in sample_sites:
        mini_meta = meta_df.query(f"population == '{site}'")
        site2samples[site] = list(mini_meta['sample'])
    return site2samples


def create_site2samples(options, paths_helper):
    if options.dataset_name == 'hgdp':
        return hgdp_create_site2samples(options, paths_helper)


def create_vcf_per_site(paths_helper):
    with open(f"{paths_helper.sfs_dir}site2sample.json", "r") as f:
        site2sample = json.load(f)
    for site, samples in site2sample.items():
        if os.path.exists(f'{paths_helper.sfs_dir}{site}/{site}.vcf.gz') and os.path.exists(f'{paths_helper.sfs_dir}{site}/{site}.vcf.gz.tbi'):
            continue
        vcf_file = f"{paths_helper.data_dir}hgdp_wgs.20190516.full.chr21.vcf"
        with Timer(f"Create VCF for site {site}"):
            os.makedirs(f'{paths_helper.sfs_dir}{site}', exist_ok=True)
            bcftools_cmd = ["bcftools", 'view', "-s", f"{','.join(samples)}", "--max-alleles", "2", "-O", "z",
                            "--min-alleles", '2', '--output-file', f'{paths_helper.sfs_dir}{site}/{site}_tmp.vcf.gz', vcf_file]
            subprocess.run([paths_helper.submit_helper, ' '.join(bcftools_cmd)])

            subprocess.run([paths_helper.submit_helper, f'bcftools filter -O z -o {paths_helper.sfs_dir}{site}/{site}.vcf.gz -i "F_MISSING=0" {paths_helper.sfs_dir}{site}/{site}_tmp.vcf.gz'])
            subprocess.run([paths_helper.submit_helper, f'tabix -p vcf {paths_helper.sfs_dir}{site}/{site}.vcf.gz'])
            os.remove(f'{paths_helper.sfs_dir}{site}/{site}_tmp.vcf.gz')

def create_vcf_per_2_sites(options, paths_helper, site, special_list):
    sites_list = get_sample_site_list(options, paths_helper)
    site_vcf_file = f"{paths_helper.sfs_dir}{site}/{site}.vcf.gz'"
    idx = sites_list.index(site)
    for other_site in sites_list[idx + 1:]:
        if other_site not in special_list:
            continue
        print(f"start with {site} & {other_site}")
        other_site_vcf_file = f'{paths_helper.sfs_dir}{other_site}/{other_site}.vcf.gz'
        combined_sites_vcf_file_tmp = f'{paths_helper.sfs_dir}{site}/{site}-{other_site}_tmp.vcf.gz'
        combined_sites_vcf_file = f'{paths_helper.sfs_dir}{site}/{site}-{other_site}.vcf.gz'
        if os.path.exists(combined_sites_vcf_file):
            continue
        bcftools_cmd = ['bcftools', 'merge', f'{site_vcf_file}', f'{other_site_vcf_file}', '-O', 'z', '-o', combined_sites_vcf_file_tmp]
        print(bcftools_cmd)
        subprocess.run([paths_helper.submit_helper, ' '.join(bcftools_cmd)])
        print("Done generate tmp vcf")
        subprocess.run([paths_helper.submit_helper, f'bcftools filter -O z -o {combined_sites_vcf_file} -i "F_MISSING=0" {combined_sites_vcf_file_tmp}'])
        os.remove(f'{combined_sites_vcf_file_tmp}')


def main():
    arguments = args_parser()
    paths_helper = get_paths_helper(arguments.dataset_name)
    os.makedirs(paths_helper.sfs_dir, exist_ok=True)
    if not os.path.exists(paths_helper.sfs_dir + 'subpopulations_histogram.svg'):
        plot_subpopulations_size_histogram(arguments, paths_helper)
    if not os.path.exists(paths_helper.sfs_dir + 'site2sample.json'):
        site2sample = create_site2samples(arguments, paths_helper)
        with open(f"{paths_helper.sfs_dir}site2sample.json", "w") as f:
            json.dump(site2sample, f)

    # create_vcf_per_site(paths_helper)

    sites_list = get_sample_site_list(arguments, paths_helper)
    special_list = ['Mandenka', 'Mbuti', 'BantuKenya', 'Yoruba', 'Biaka']
    for site in sites_list:
        if site not in special_list:
            continue
        create_vcf_per_2_sites(arguments, paths_helper, site, special_list)


if __name__ == '__main__':
    main()