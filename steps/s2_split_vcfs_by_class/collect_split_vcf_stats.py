# collects stats from the split vcfs by class
# TOD rename? - collect_vcf_classes_stats?
import re
import sys
import os.path
import sys
import os
from os import path
from os.path import dirname, abspath


root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)

from utils.loader import Timer
from utils.common import *
from utils.config import *
from utils.checkpoint_helper import *
from steps.s2_split_vcfs_by_class import submit_split_vcfs_by_class

SCRIPT_NAME = os.path.basename(__file__)

def get_split_vcf_stats(filepath, chr_name):
    # extract stats from the stderr file
    # return a dictionary
    values = dict()
    values['chr_name'] = chr_name
    values['mac'] = '-'
    values['maf'] = '-'
    values['max_mac'] = '-'
    values['max_maf'] = '-'
    print(filepath)
    #regexes
    # After filtering, kept 929 out of 929 Individuals
    r = r'After filtering, kept (\d+) out of (\d+) Individuals'
    indv_regex = re.compile(r)
    # After filtering, kept 3202 out of a possible 1185008 Sites
    r = r'After filtering, kept (\d+) out of a possible (\d+) Sites'
    sites_regex = re.compile(r)
    # Run Time = 489.00 seconds
    r = r'Run Time = (\d+).00 seconds'
    time_regex = re.compile(r)

    with open(filepath) as fp:
        for cnt, line in enumerate(fp):
            if ('--gzvcf ' in line):
                values['input_file'] = line.split('--gzvcf ')[1].strip()
                continue
            elif ('--mac ' in line):
                values['mac'] = line.split('--mac ')[1].strip()
                continue
            elif ('--max-mac ' in line):
                values['max_mac'] = line.split('--max-mac ')[1].strip()
                continue
            elif ('--maf ' in line):
                values['maf'] = line.split('--maf ')[1].strip()
                continue
            elif ('--max-maf ' in line):
                values['max_maf'] = line.split('--max-maf ')[1].strip()
                continue
            elif ('--out ' in line):
                values['out_path'] = line.split('--out ')[1].strip()
                continue
            else:
                match = indv_regex.match(line)
                if match:
                    values['num_of_indv_after_filter'] = match.groups()[0]
                    values['num_of_possible_indv'] = match.groups()[1]
                    continue
                match = sites_regex.match(line)
                if match:
                    values['num_of_sites_after_filter'] = match.groups()[0]
                    values['num_of_possible_sites'] = match.groups()[1]
                    continue
                match = time_regex.match(line)
                if match:
                    values['run_time_in_seconds'] = match.groups()[0]

    # based on the values we analyze the output files
    # analyze indv file
    indv_file = values['out_path'] + '.012.indv'
    values['indv_num_of_lines'] = get_num_lines_in_file(indv_file)

    pos_file = values['out_path'] + '.012.pos'
    values['pos_num_of_lines'] = get_num_lines_in_file(pos_file)

    main_file = values['out_path'] + '.012'
    values['012_num_of_lines'] = get_num_lines_in_file(main_file)

    min_c, max_c = min_max_number_of_columns(main_file)
    # substract 1 as we have an index column
    values['012_min_num_of_sites'] = min_c-1
    values['012_max_num_of_sites'] = max_c-1
    return values

def min_max_number_of_columns(file_path):
    min_c = sys.maxsize
    max_c = -1
    for line in open(file_path).readlines():
        c = len(line.split('\t'))
        if c < min_c:
            min_c = c
        if c > max_c:
            max_c = c
    return min_c, max_c

def write_values_to_csv(values, output_path):
    # first, assert we have all values
    expected_keys = ['chr_name', 'mac', 'max_mac', 'maf', 'max_maf', 'num_of_indv_after_filter', 'indv_num_of_lines',
                     '012_num_of_lines', 'num_of_possible_indv', 'num_of_sites_after_filter', 'pos_num_of_lines',
                     '012_min_num_of_sites', '012_max_num_of_sites', 'num_of_possible_sites', 'run_time_in_seconds',
                     'input_file', 'out_path']
    values_keys = values.keys()
    for exp_key in expected_keys:
        assert exp_key in values_keys
    write_header = not os.path.isfile(output_path)
    with open(output_path, "a+") as f:
        if write_header:
            f.write(','.join(expected_keys) + '\n')
        # if file doesnt exist, write the header
        f.write(','.join([str(values[k]) for k in expected_keys]) + '\n')


# TODO renmae? collect_vcf_classes_stats
def collect_split_vcf_stats(log_files, chr_names, split_vcf_stats_csv_path):
    assert len(log_files) == len(chr_names)
    for i in range(len(log_files)):
        chr_name = chr_names[i]
        log_file = log_files[i]
        values = get_split_vcf_stats(log_file, chr_name)
        write_values_to_csv(values, split_vcf_stats_csv_path)
        print(f'done with file {i} out of {len(log_files)} - {log_file}')

# hgdp_text, 2, 8, 1, 49
# TODO renmae? collect_and_validate_vcf_classes_stats
def call_collect_split_vcf_stats(options):
    dataset_name = options.dataset_name
    min_mac_range, max_mac_range = options.mac
    min_maf_range, max_maf_range = options.maf
    paths_helper = get_paths_helper(dataset_name)
    split_vcf_stats_csv_path = paths_helper.split_vcf_stats_csv_path
    vcf_file_short_names = get_dataset_vcf_files_short_names(dataset_name)
    macs = range(min_mac_range, max_mac_range+1)
    #mafs = ["{0:.2f}".format(float(v)/100) for v in range(min_maf_range,max_maf_range+1)]
    mafs = range(min_maf_range, max_maf_range+1)
    log_files = []
    chr_names_for_logs = []
    job_type = submit_split_vcfs_by_class.job_type
    for vcf_file_short_name in vcf_file_short_names:
        for mac in macs:
            job_long_name = submit_split_vcfs_by_class.generate_job_long_name('mac', mac, vcf_file_short_name)
            job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name=job_long_name)
            log_files.append(job_stderr_file)
            chr_names_for_logs.append(vcf_file_short_name)
        for maf in mafs:
            job_long_name = submit_split_vcfs_by_class.generate_job_long_name('maf', maf, vcf_file_short_name)
            job_stderr_file = paths_helper.logs_cluster_jobs_stderr_template.format(job_type=job_type, job_name=job_long_name)
            log_files.append(job_stderr_file)
            chr_names_for_logs.append(vcf_file_short_name)

    print(f'will process {len(log_files)} files')

    collect_split_vcf_stats(log_files, chr_names_for_logs, split_vcf_stats_csv_path)
    return validate_split_vcf_output_stats_file(options=options,
                                                split_vcf_output_stats_file=split_vcf_stats_csv_path)


def validate_split_vcf_output_stats_file(options, split_vcf_output_stats_file):
    num_ind = get_num_individuals(options.dataset_name)
    min_mac, max_mac = options.mac
    min_maf, max_maf = options.maf
    min_chr = get_min_chr(options.dataset_name)
    max_chr = get_max_chr(options.dataset_name)
    df = pd.read_csv(split_vcf_output_stats_file)
    df['mac_or_maf'] = df.apply(lambda r : r['mac'] if r['mac']!='-' else r['maf'], axis=1)

    # first validate all data is here
    assert validate_all_data_exists(df, max_chr, max_mac, max_maf, min_chr, min_mac, min_maf)
    print(f'PASSED - all chrs has all relevant macs and mafs once')

    # next validate all have the correct num_ind
    assert validate_correct_individual_num(df, num_ind)
    print(f'PASSED - all entries has the correct num of individuals ({num_ind})')

    # next validate same num_of_possible_sites per chr
    assert validate_num_of_possible_sites(df)
    print('PASSED - all chrs has the same number of possilbe sites')

    # validate the number of sites after filtering is indeed the number of sites in the 012 file
    assert validate_num_of_sites_comp_to_012(df)
    print('PASSED - number of sites in 012 files matches that of vcftools output')

    # validate per chr and class we have a single line
    # todo - I think we check it previously in validate_all_data_exists()
    chr_class_df = df.groupby(['chr_name', 'mac_or_maf'])['mac'].count().reset_index()
    assert (len(chr_class_df[chr_class_df['mac'] != 1]) == 0)
    print('PASSED - single line per chr and name')
    return True


def validate_num_of_possible_sites(df):
    passed = True
    grouped = df.groupby('chr_name')['num_of_possible_sites'].agg('nunique').reset_index()
    cond = len(grouped[grouped['num_of_possible_sites'] != 1]) == 0
    if not cond:
        print(f'a chr with different number of num_of_possible_sites is found')
        print(grouped[grouped['num_of_possible_sites'] != 1])
        passed = False
    return passed


def validate_num_of_sites_comp_to_012(df):
    passed = True
    df['validate012sites'] = (df['num_of_sites_after_filter'] == df['pos_num_of_lines']) & \
                             (df['num_of_sites_after_filter'] == df['012_min_num_of_sites']) & \
                             (df['num_of_sites_after_filter'] == df['012_max_num_of_sites'])
    cond = len(df[~df['validate012sites']]) == 0
    if not cond:
        print(f'number of sites after filtering doesnt match 012 file')
        print(df[~df['validate012sites']])
        passed = False
    return passed


def validate_correct_individual_num(df, num_ind):
    passed = True
    for c in ['num_of_indv_after_filter', 'indv_num_of_lines', '012_num_of_lines', 'num_of_possible_indv']:
        if len(df[df[c] != num_ind]) != 0:
            passed = False
            print(f'wrong number of ind for column {c}')
            print(df[df[c] != num_ind][['chr_name', c]])
    return passed


def validate_all_data_exists(df, max_chr, max_mac, max_maf, min_chr, min_mac, min_maf):
    passed = True
    for chr_i in range(min_chr, max_chr + 1):
        for mac in range(min_mac, max_mac + 1):
            count = len(df[(df['chr_name'] == f'chr{chr_i}') & (df['mac'] == f'{mac}')])
            if count != 1:
                passed = False
                print(f'chr{chr_i}, mac {mac} appears {count} times')
        for maf in range(min_maf, max_maf + 1):
            chr_df = df[(df['chr_name'] == 'chr' + str(chr_i))]
            count = len(chr_df[chr_df['maf'] == str(maf / 100.0)])
            if count != 1:
                passed = False
                print(f'chr{chr_i}, maf {maf} appears {count} times')
    return passed


def main(options):
    with Timer(f"Collect split vcf stats with {str_for_timer(options)}"):
        is_executed, msg = execute_with_checkpoint(call_collect_split_vcf_stats, SCRIPT_NAME, options)
    return is_executed


if __name__ == '__main__':
    # arguments = args_parser()
    # main(arguments)
    df_path = "C:\\Users\\lab4\\OneDrive\\Desktop\\df.xlsx"
    df = pd.read_excel(df_path)
    validate_all_data_exists(df, 22, 0, 1, 1, 1, 1)