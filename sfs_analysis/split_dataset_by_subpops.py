import json
import os
import subprocess
import time
from io import StringIO
from os.path import dirname, abspath
import sys
from tqdm import tqdm


root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)
from utils import config
sys.path.insert(0, f'{config.get_config(config.CONFIG_NAME_PATHS)["venv_path"]}lib/python3.7/site-packages')


from utils.cluster.cluster_helper import submit_to_cluster
from utils.similarity_helper import file012_to_numpy
from utils.loader import Timer, Loader
from scipy.ndimage.filters import gaussian_filter
from utils.config import get_indlist_file_name, get_cluster_code_folder
from utils.common import args_parser, get_paths_helper, warp_how_many_jobs, validate_stderr_empty
from sfs_analysis.sfs_utils import get_sample_site_list, get_site2size, get_theoretical_sfs

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def plot_subpopulations_size_histogram(options, paths_helper):
    vcf_dir = paths_helper.data_dir
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


def create_vcf_per_site(options, paths_helper):
    with open(f"{paths_helper.sfs_dir}summary/site2sample.json", "r") as f:
        site2sample = json.load(f)
    for site, samples in site2sample.items():
        if os.path.exists(f'{paths_helper.sfs_dir_chr}{site}/{site}.vcf.gz') and os.path.exists(f'{paths_helper.sfs_dir_chr}{site}/{site}.vcf.gz.tbi'):
            continue

        vcf_file = f"{paths_helper.data_dir}hgdp_wgs.20190516.full.chr{options.chr_num}.vcf.gz"
        with Timer(f"Create VCF for site {site}"):
            os.makedirs(f'{paths_helper.sfs_dir_chr}{site}', exist_ok=True)
            bcftools_cmd = ["bcftools", 'view', "-s", f"{','.join(samples)}", "--max-alleles", "2", "-O", "z",
                            "--min-alleles", '2', '--output-file', f'{paths_helper.sfs_dir_chr}{site}/{site}_tmp.vcf.gz', vcf_file]
            subprocess.run([paths_helper.submit_helper, ' '.join(bcftools_cmd)])

            subprocess.run([paths_helper.submit_helper, f'bcftools filter -O z -o {paths_helper.sfs_dir_chr}{site}/{site}.vcf.gz -i "F_MISSING=0" {paths_helper.sfs_dir_chr}{site}/{site}_tmp.vcf.gz'])
            subprocess.run([paths_helper.submit_helper, f'tabix -p vcf {paths_helper.sfs_dir_chr}{site}/{site}.vcf.gz'])
            os.remove(f'{paths_helper.sfs_dir_chr}{site}/{site}_tmp.vcf.gz')

def create_vcf_per_2_sites(options, paths_helper, site, special_list):
    sites_list = get_sample_site_list(options, paths_helper)
    site_vcf_file = f'{paths_helper.sfs_dir_chr}{site}/{site}.vcf.gz'
    idx = sites_list.index(site)
    for other_site in sites_list[idx + 1:]:
        if other_site not in special_list:
            continue
        other_site_vcf_file = f'{paths_helper.sfs_dir_chr}{other_site}/{other_site}.vcf.gz'
        combined_sites_vcf_file_tmp = f'{paths_helper.sfs_dir_chr}{site}/{site}-{other_site}_tmp.vcf.gz'
        combined_sites_vcf_file = f'{paths_helper.sfs_dir_chr}{site}/{site}-{other_site}.vcf.gz'
        if os.path.exists(combined_sites_vcf_file):
            continue
        print(f"Run {site} & {other_site}")
        bcftools_cmd = ['bcftools', 'merge', f'{site_vcf_file}', f'{other_site_vcf_file}', '-O', 'z', '-o', combined_sites_vcf_file_tmp]
        subprocess.run([paths_helper.submit_helper, ' '.join(bcftools_cmd)])
        subprocess.run([paths_helper.submit_helper, f'bcftools filter -O z -o {combined_sites_vcf_file} -i "F_MISSING=0" {combined_sites_vcf_file_tmp}'])
        os.remove(f'{combined_sites_vcf_file_tmp}')


def convert_012_to_sfs(matrix_012_file_path, site_size, other_site_size):
    np_matrix = file012_to_numpy(matrix_012_file_path)
    num_of_genomes = 2 * (site_size + other_site_size)
    assert np_matrix.min() == 0 and np_matrix.max() <= 2
    assert np_matrix.shape[0] == num_of_genomes / 2
    matrix_minor_count = np.sum(np_matrix, axis=0)
    matrix_minor_count = np.minimum(matrix_minor_count, num_of_genomes - matrix_minor_count)
    hst = np.histogram(matrix_minor_count, bins=num_of_genomes // 2 + 1)
    return hst[0]

def vcf2matrix2sfs(options, paths_helper, sites_list):
    print("### Start stage 3 ###")
    site2size = get_site2size(paths_helper)
    for idx, site in enumerate(tqdm(sites_list)):
        for other_site in tqdm(sites_list[idx + 1:], leave=False):
            vcf_file_path = f'{paths_helper.sfs_dir_chr}{site}/{site}-{other_site}'
            if not os.path.exists(f'{vcf_file_path}.012'):
                print(f"Run {site} & {other_site}")
                vcftools_cmd = f'vcftools --gzvcf {vcf_file_path}.vcf.gz --012 --out {vcf_file_path}'
                subprocess.run([paths_helper.submit_helper, vcftools_cmd])
            hst_file_name = f'{paths_helper.sfs_dir_chr}{site}/{site}-{other_site}-hst.npy'
            if not os.path.exists(hst_file_name):
                site_size = site2size[site]
                other_site_size = site2size[other_site]
                hst = convert_012_to_sfs(f'{vcf_file_path}.012', site_size, other_site_size)
                np.save(hst_file_name, hst)


def create_heat_map(options, paths_helper, sites_list):
    print("### Stage 4 ###")
    sites_size = get_site2size(paths_helper)
    num_of_sites = len(sites_list)
    hists = {}
    relative_heat = pd.DataFrame(columns=sites_list, index=sites_list)
    theoretical_heat = pd.DataFrame(columns=sites_list, index=sites_list)
    for idx1, site in enumerate(tqdm(sites_list)):
        relative_heat.at[site, site] = 0
        theoretical_heat.at[site, site] = 0
        for idx2, other_site in enumerate(sites_list):
            if idx1 >= idx2:
                continue
            hst_path = f'{paths_helper.sfs_dir_chr}{site}/{site}-{other_site}-hst.npy'
            hst = np.load(hst_path)
            hst[-1] *= 2
            hists[f'{site}-{other_site}'] = hst.tolist()
            hot_spot_idx = 2 * (min(sites_size[site], sites_size[other_site]))
            theoretical = get_theoretical_sfs(np.sum(hst[1:]), sites_size[site] + sites_size[other_site])
            assert hot_spot_idx <= hst.size - 1
            if hot_spot_idx < hst.size - 1:
                divider = np.sqrt(hst[hot_spot_idx - 1] * hst[hot_spot_idx + 1]) if hst[hot_spot_idx - 1] * hst[hot_spot_idx + 1] > 0 else 1
                res = hst[hot_spot_idx] / divider
            else:
                divider = hst[hot_spot_idx - 1] if hst[hot_spot_idx - 1] > 0 else 1
                res = hst[hot_spot_idx] / divider
            relative_heat.at[site, other_site] = res
            relative_heat.at[other_site, site] = res
            theoretical_heat.at[site, other_site] = hst[hot_spot_idx] / theoretical[hot_spot_idx - 1]
            theoretical_heat.at[other_site, site] = hst[hot_spot_idx] / theoretical[hot_spot_idx - 1]
    with open(f"{paths_helper.sfs_dir_chr}summary/all_hists.json", "w") as f:
        json.dump(hists, f)

    relative_heat.to_csv(f'{paths_helper.sfs_dir_chr}summary/relative_heat.csv', index_label="sites")
    theoretical_heat.to_csv(f'{paths_helper.sfs_dir_chr}summary/theoretical_heat.csv', index_label="sites")


def submit_all_sites(options, paths_helper):
    sites_list = get_sample_site_list(options, paths_helper)
    errs = []
    for idx, site in enumerate(sites_list):
        combine_files = [f'{paths_helper.sfs_dir_chr}{site}/{site}-{other_site}.vcf.gz' for other_site in sites_list[idx + 1:]]
        if all([os.path.exists(path) for path in combine_files]):
            continue
        script_args = f'-d {options.dataset_name} --args {site} --chr {options.chr_num}'
        job_type = 'sfs_analysis'
        job_name = f'vcf_{site}'
        job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                job_name=job_name)
        job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                                job_name=job_name)
        errs.append(job_stderr_file)
        submit_to_cluster(options, job_type='sfs_analysis', job_name=f'vcf_{site}', script_args=script_args,
                          job_stdout_file=job_stdout_file, job_stderr_file=job_stderr_file, num_hours_to_run=12,
                          script_path=f'{get_cluster_code_folder()}snpnmi/sfs_analysis/split_dataset_by_subpops.py')

    jobs_func = warp_how_many_jobs('vcf')
    with Loader("Merging VCF files for pairwise sub-populations", jobs_func):
        while jobs_func():
            time.sleep(5)

    assert validate_stderr_empty(errs)

def multichromosome_stats(options, paths_helper):

    os.makedirs(paths_helper.sfs_dir, exist_ok=True)
    os.makedirs(paths_helper.sfs_dir + 'summary', exist_ok=True)
    if not os.path.exists(f"{paths_helper.sfs_dir}summary/subpopulations_histogram.svg"):
        plot_subpopulations_size_histogram(options, paths_helper)
    if not os.path.exists(f"{paths_helper.sfs_dir}summary/site2sample.json"):
        site2sample = create_site2samples(options, paths_helper)
        with open(f"{paths_helper.sfs_dir}summary/site2sample.json", "w") as f:
            json.dump(site2sample, f)
        with open(f"{paths_helper.sfs_dir}summary/site2size.json", "w") as f:
            json.dump({k: len(v) for (k, v) in site2sample.items()}, f)

def main():
    arguments = args_parser()
    paths_helper = get_paths_helper(arguments.dataset_name)
    multichromosome_stats(arguments, paths_helper)
    paths_helper.sfs_dir_chr = paths_helper.sfs_dir + f'chr{arguments.chr_num}/'
    os.makedirs(paths_helper.sfs_dir, exist_ok=True)
    os.makedirs(paths_helper.sfs_dir_chr, exist_ok=True)
    os.makedirs(f'{paths_helper.sfs_dir_chr}/summary', exist_ok=True)

    if arguments.args:
        sites_list = get_sample_site_list(arguments, paths_helper)
        create_vcf_per_2_sites(arguments, paths_helper, arguments.args[0], sites_list)
        return True

    create_vcf_per_site(arguments, paths_helper)
    submit_all_sites(arguments, paths_helper)
    sites_list = get_sample_site_list(arguments, paths_helper)
    vcf2matrix2sfs(arguments, paths_helper, sites_list)
    create_heat_map(arguments, paths_helper, sites_list)


if __name__ == '__main__':
    main()
