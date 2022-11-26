import json
import os
import subprocess
import time
from io import StringIO
from os.path import dirname, abspath
import sys
from tqdm import tqdm
import seaborn as sns

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
from matplotlib.lines import Line2D
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
    removed_list = 0
    for site in indlist:
        if site == "Removed":
            removed_list += 1
            continue
        site_hist[site] += 1
    assert sum(site_hist.values()) == len(indlist) - removed_list
    freq_hist = np.zeros(max(site_hist.values()))
    for count in site_hist.values():
        freq_hist[count - 1] += 1
    plt.plot(np.arange(freq_hist.size) + 1, gaussian_filter(freq_hist, 1))
    plt.scatter(np.arange(freq_hist.size) + 1, gaussian_filter(freq_hist, 1), s=10)
    plt.title(f"Histogram of subpopulation sizes in {options.dataset_name}")
    plt.xlabel("Number of individuals")
    plt.ylabel("Number of subpopulations")
    plt.savefig(f"{paths_helper.sfs_dir}summary/subpopulations_histogram.svg")
    plt.clf()


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

def arabidopsis_create_site2samples(options, paths_helper):
    meta_data_file_path = paths_helper.data_dir + 'full_sample_sites.csv'
    meta_df = pd.read_csv(meta_data_file_path)
    sample_sites = get_sample_site_list(options, paths_helper)
    site2samples = {}
    for site in sample_sites:
        mini_meta = meta_df.query(f"Country == '{site}'")
        site2samples[site] = list(mini_meta['Individual_name'])
    return site2samples

def create_site2samples(options, paths_helper):
    if options.dataset_name == 'hgdp':
        return hgdp_create_site2samples(options, paths_helper)
    if options.dataset_name == 'arabidopsis':
        return arabidopsis_create_site2samples(options, paths_helper)


def create_vcf_per_site(options, site, paths_helper):
    with open(f"{paths_helper.sfs_dir}summary/site2sample.json", "r") as f:
        site2sample = json.load(f)
    samples = site2sample[site]
    if os.path.exists(f'{paths_helper.sfs_dir_chr}{site}/{site}.vcf.gz') and os.path.exists(f'{paths_helper.sfs_dir_chr}{site}/{site}.vcf.gz.tbi'):
        return True
    if options.dataset_name == 'hgdp':
            vcf_file = f"{paths_helper.data_dir}hgdp_wgs.20190516.full.chr{options.chr_num}.vcf.gz"
    if options.dataset_name == 'arabidopsis':
            vcf_file = f"{paths_helper.data_dir}africa_and1001.EVA.vcf.gz"
    with Timer(f"Create VCF for site {site}"):
        os.makedirs(f'{paths_helper.sfs_dir_chr}{site}', exist_ok=True)
        bcftools_cmd = ["bcftools", 'view', "-s", f"{','.join(samples)}", "--max-alleles", "2", "-O", "z",
                        "--min-alleles", '2', '--output-file', f'{paths_helper.sfs_dir_chr}{site}/{site}_tmp.vcf.gz', vcf_file]
        subprocess.run([paths_helper.submit_helper, ' '.join(bcftools_cmd)])

        subprocess.run([paths_helper.submit_helper, f'bcftools filter -O z -o {paths_helper.sfs_dir_chr}{site}/{site}.vcf.gz -i "F_MISSING=0" {paths_helper.sfs_dir_chr}{site}/{site}_tmp.vcf.gz'])
        subprocess.run([paths_helper.submit_helper, f'tabix -p vcf {paths_helper.sfs_dir_chr}{site}/{site}.vcf.gz'])
        os.remove(f'{paths_helper.sfs_dir_chr}{site}/{site}_tmp.vcf.gz')

def create_vcf_per_2_sites(options, paths_helper, site, sites_list):
    site_vcf_file = f'{paths_helper.sfs_dir_chr}{site}/{site}.vcf.gz'
    idx = sites_list.index(site)
    for other_site in sites_list[idx + 1:]:
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
    hst = np.histogram(matrix_minor_count, bins=np.arange(num_of_genomes // 2 + 2))
    return hst[0]


def vcf2matrix2sfs(options, paths_helper, site, sites_list):
    site2size = get_site2size(paths_helper)
    idx = sites_list.index(site)
    for other_site in sites_list[idx + 1:]:
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
            gap = 2 if options.dataset_name == 'arabidopsis' else 1
            assert hot_spot_idx <= hst.size - 1
            if hot_spot_idx <= gap:
                if hot_spot_idx >= hst.size - gap * 2:
                    divider = np.nan
                else:
                    divider = hst[hot_spot_idx + gap] ** 2 / hst[hot_spot_idx + gap * 2]
            elif hot_spot_idx >= hst.size - gap:
                if hot_spot_idx <= gap * 2:
                    divider = np.nan
                else:
                    divider = hst[hot_spot_idx - gap] ** 2 / hst[hot_spot_idx - gap * 2]
            else:
                divider = np.sqrt(hst[hot_spot_idx - gap] * hst[hot_spot_idx + gap]) if hst[hot_spot_idx - gap] * hst[hot_spot_idx + gap] > 0 else np.nan
            res = hst[hot_spot_idx] / divider
            relative_heat.at[site, other_site] = res
            relative_heat.at[other_site, site] = res
            theoretical_heat.at[site, other_site] = hst[hot_spot_idx] / theoretical[hot_spot_idx - gap]
            theoretical_heat.at[other_site, site] = hst[hot_spot_idx] / theoretical[hot_spot_idx - gap]
    with open(f"{paths_helper.sfs_dir_chr}summary/all_hists.json", "w") as f:
        json.dump(hists, f)

    relative_heat.to_csv(f'{paths_helper.sfs_dir_chr}summary/relative_heat.csv', index_label="sites")
    theoretical_heat.to_csv(f'{paths_helper.sfs_dir_chr}summary/theoretical_heat.csv', index_label="sites")


def submit_all_sites(options, paths_helper, run_step):
    sites_list = get_sample_site_list(options, paths_helper)
    errs = []
    for idx, site in enumerate(sites_list):
        combine_files = [f'{paths_helper.sfs_dir_chr}{site}/{site}-{other_site}.vcf.gz' for other_site in sites_list[idx + 1:]]
        if run_step == 2 and all([os.path.exists(path) for path in combine_files]):
            continue
        if run_step == 1 and os.path.exists(f'{paths_helper.sfs_dir_chr}{site}/{site}.vcf.gz'):
            continue
        combine_files = [f'{paths_helper.sfs_dir_chr}{site}/{site}-{other_site}-hst.npy' for other_site in
                         sites_list[idx + 1:]]
        if run_step == 3 and all([os.path.exists(path) for path in combine_files]):
            continue
        script_args = f'-d {options.dataset_name} --args {site} --chr {options.chr_num}'
        job_type = 'sfs_analysis'
        job_name = f'vcf_{run_step}_{site}'
        job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type,
                                                                                job_name=job_name)
        job_stdout_file = paths_helper.logs_cluster_jobs_stdout_template.format(job_type=job_type,
                                                                                job_name=job_name)
        errs.append(job_stderr_file)
        submit_to_cluster(options, job_type='sfs_analysis', job_name=f'vcf_{site}', script_args=script_args,
                          job_stdout_file=job_stdout_file, job_stderr_file=job_stderr_file, num_hours_to_run=12,
                          script_path=f'{get_cluster_code_folder()}SNPnmi/sfs_analysis/split_dataset_by_subpops.py')

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


def compare_heatmap_to_fst(options, paths_helper, fst_file_name):
    heat_map_df = pd.read_csv(f'{paths_helper.sfs_dir_chr}summary/relative_heat.csv')
    fst_x = []
    heatmap_y = []
    colors = []
    continents_dict = {}
    with open(f'{paths_helper.data_dir}pops.txt', 'r') as f:
        continents_txt = f.read()
        for line in continents_txt.split('\n')[:-1]:
            continents_dict[line.split(' ')[0]] = line.split(' ')[1]

    with open(f'{paths_helper.sfs_dir}/summary/{fst_file_name}', 'r') as f:
        for line in tqdm(f.readlines()):
            line_lst = line.split(' ')
            assert len(line_lst) == 3
            heatmap_y.append(float(heat_map_df[heat_map_df['sites'] == line_lst[0]][line_lst[1]]))
            fst_x.append(float(line_lst[2]))
            assert line_lst[0] in continents_dict.keys() and line_lst[1] in continents_dict.keys()
            if any([continents_dict[line_lst[i]] != 'AFRICA' and continents_dict[line_lst[1-i]] == 'AFRICA'
                    for i in [0, 1]]):
                colors.append('tab:orange')
            else:
                colors.append('tab:blue')
    fst_x = np.array(fst_x)
    heatmap_y = np.array(heatmap_y)
    # first_element = np.sum((fst_x - np.mean(fst_x)) @ (heatmap_y - np.sum(heatmap_y)))
    # second_element = np.sum((fst_x - np.mean(fst_x)) ** 2) * np.sum((heatmap_y - np.mean(heatmap_y)) ** 2)
    # R = first_element / np.sqrt(second_element)
    # assert np.abs(R - np.corrcoef(fst_x, heatmap_y)[0, 1]) <= 0.001, R - np.corrcoef(fst_x, heatmap_y)[0, 1]
    plt.scatter(x=fst_x, y=heatmap_y, c=colors, s=2)
    plt.xlabel("Pairwise Fst score")
    plt.ylabel("Peak score")
    plt.title(f"Peak score and Fst")
    plt.legend(handles=[Line2D([0], [0], color='w', markerfacecolor='tab:blue', marker='o', label='General'),
                        Line2D([0], [0], color='w', markerfacecolor='tab:orange', marker='o', label='Africa')])
    plt.savefig(f'{paths_helper.sfs_dir_chr}/summary/relative2fst_plot.svg')


def violin_plot(options, paths_helper):
    continents_dict = {}
    with open(f'{paths_helper.data_dir}pops.txt', 'r') as f:
        continents_txt = f.read()
        for line in continents_txt.split('\n')[:-1]:
            continents_dict[' '.join(line.split(' ')[:-1])] = line.split(' ')[-1]
    within_regions = []
    across_regions = []
    heat_map_df = pd.read_csv(f'{paths_helper.sfs_dir_chr}summary/relative_heat.csv')
    pops = list(heat_map_df.iloc[0].index)[1:]
    for idx1, row in heat_map_df.iterrows():
        pop1 = row[0]
        for idx2, val in enumerate(row[idx1 + 2:], start=idx1 + 1):
            if continents_dict[pop1] == continents_dict[pops[idx2]]:
                within_regions.append(val)
            else:
                across_regions.append(val)
    colors = ['tab:blue', 'tab:orange']
    sns.violinplot(data=[np.array(within_regions).astype('float64'),
                    np.array(across_regions).astype('float64')],
                   palette=colors)
    plt.xticks([0, 1], ['Within regions', 'Across regions'])
    plt.ylabel("Peak score")
    plt.title(f"Peak Score withing regions and across regions at {options.dataset_name}")
    plt.savefig(f'{paths_helper.sfs_dir_chr}/summary/violin.svg')


def main():
    arguments = args_parser()
    paths_helper = get_paths_helper(arguments.dataset_name)
    paths_helper.sfs_dir_chr = paths_helper.sfs_dir + f'chr{arguments.chr_num}/'
    multichromosome_stats(arguments, paths_helper)
    os.makedirs(paths_helper.sfs_dir, exist_ok=True)
    os.makedirs(paths_helper.sfs_dir_chr, exist_ok=True)
    os.makedirs(f'{paths_helper.sfs_dir_chr}/summary', exist_ok=True)
    run_step = 3
    if arguments.args:
        sites_list = get_sample_site_list(arguments, paths_helper)
        if run_step == 1:
            create_vcf_per_site(arguments, arguments.args[0], paths_helper)
        if run_step == 2:
            create_vcf_per_2_sites(arguments, paths_helper, arguments.args[0], sites_list)
        if run_step == 3:
            vcf2matrix2sfs(arguments, paths_helper, arguments.args[0], sites_list)
        return True

    # submit_all_sites(arguments, paths_helper, run_step)
    # create_heat_map(arguments, paths_helper, sites_list)
    # compare_heatmap_to_fst(arguments, paths_helper, 'hgdp_fst.txt')
    violin_plot(arguments, paths_helper)


if __name__ == '__main__':
    main()
