# collects stats from the split vcfs by class
# TODO - add validation, see notebook Validate split_vcf output.ipynb
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
from utils.common import *
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

    r = r'After filtering, kept (\d+) out of (\d+) Individuals'
    indv_regex = re.compile(r)
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
    expected_keys = ['chr_name', 'mac', 'max_mac', 'maf', 'max_maf', 'num_of_indv_after_filter', 'indv_num_of_lines', '012_num_of_lines', 'num_of_possible_indv', 'num_of_sites_after_filter', 'pos_num_of_lines', '012_min_num_of_sites', '012_max_num_of_sites', 'num_of_possible_sites', 'run_time_in_seconds', 'input_file', 'out_path']
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
    min_mac_range, max_mac_range, min_maf_range, max_maf_range = options.args
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
    # TODO - Shahar? Add here validate_split_vcf_classes_stat()

    print(f'will process {len(log_files)} files')

    collect_split_vcf_stats(log_files, chr_names_for_logs, split_vcf_stats_csv_path)

def _test_me():
    call_collect_split_vcf_stats(DataSetNames.hdgp_test, 20, 18, 1, 2)
#_test_me()

def main(options):
    s = time.time()
    is_executed, msg = execute_with_checkpoint(call_collect_split_vcf_stats, SCRIPT_NAME, options)
    print(f'{msg}. {(time.time()-s)/60} minutes total run time')
    return is_executed

# dataset_name, mac_min_range, mac_max_range, maf_min_range, maf_max_range
if __name__ == '__main__':
    main(sys.argv[1:])
