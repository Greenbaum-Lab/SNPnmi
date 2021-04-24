import re
import sys
import os.path

def get_split_vcf_stats(logs_folder, log_file, chr_name_name):
    # extract stats from the stderr file
    # return a dictionary
    filepath = logs_folder + log_file
    values = dict()
    values['chr_name_name'] = chr_name_name
    values['mac'] = '-'
    values['maf'] = '-'
    values['max_mac'] = '-'
    values['max_maf'] = '-'
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
    values['indv_num_of_lines'] = number_of_lines(indv_file)

    pos_file = values['out_path'] + '.012.pos'
    values['pos_num_of_lines'] = number_of_lines(pos_file)

    main_file = values['out_path'] + '.012'
    values['012_num_of_lines'] = number_of_lines(main_file)

    min_c, max_c = min_max_number_of_columns(main_file)
    # substract 1 as we have an index column
    values['012_min_num_of_sites'] = min_c-1
    values['012_max_num_of_sites'] = max_c-1
    return values

# TODO move to common?
def number_of_lines(file_path):
    count = 0
    for line in open(file_path).readlines(): count += 1
    return count

# TODO move to common?
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
    expected_keys = ['chr_name_name', 'mac', 'max_mac', 'maf', 'max_maf', 'num_of_indv_after_filter', 'indv_num_of_lines', '012_num_of_lines', 'num_of_possible_indv', 'num_of_sites_after_filter', 'pos_num_of_lines', '012_min_num_of_sites', '012_max_num_of_sites', 'num_of_possible_sites', 'run_time_in_seconds', 'input_file', 'out_path']
    values_keys = values.keys()
    for exp_key in expected_keys:
        assert exp_key in values_keys
    write_header = not os.path.isfile(output_path)
    with open(output_path, "a+") as f:
        if write_header:
            f.write(','.join(expected_keys) + '\n')
        # if file doesnt exist, write the header
        f.write(','.join([str(values[k]) for k in expected_keys]) + '\n')


def collect_split_vcf_stats(logs_folder, log_files, chr_name_names, split_vcf_stats_csv_path):
    assert len(log_files) == len(chr_name_names)
    for i in range(len(log_files)):
        chr_name_name = chr_name_names[i]
        log_file = log_files[i]
        values = get_split_vcf_stats(logs_folder, log_file, chr_name_name)
        write_values_to_csv(values, split_vcf_stats_csv_path)
        print(f'done with file {i} out of {len(log_files)} - {log_file}')


def call_collect_split_vcf_stats(logs_folder, chr_names, split_vcf_stats_csv_path, min_mac_range, max_mac_range, mac_delta, min_maf_range, max_maf_range, maf_delta):
    macs = range(min_mac_range,max_mac_range+1, mac_delta)
    mafs = ["{0:.2f}".format(float(v)/100) for v in range(min_maf_range,max_maf_range+1, maf_delta)]
    log_files = []
    chr_names_for_logs = []
    for chr_name in chr_names:
        for mac in macs:
            log_files.append(f'{chr_name}_mac{mac}.stderr')
            chr_names_for_logs.append(chr_name)
        for maf in mafs:
            log_files.append(f'{chr_name}_maf{maf}.stderr')
            chr_names_for_logs.append(chr_name)
    print(f'will process {len(log_files)} files')
    collect_split_vcf_stats(logs_folder, log_files, chr_names_for_logs, split_vcf_stats_csv_path)

# TODO - add main, use paths_helper
#HGDP
logs_folder = r'/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/split_vcfs/stderr/'
# macs folder:
#logs_folder = r'/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/split_vcfs/with_upper_bound_stderr/'
chr_names = [f'chr{i}' for i in range(1,23)]
split_vcf_stats_csv_path = r'/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/split_vcfs/split_vcf_output_stats.csv'

call_collect_split_vcf_stats(logs_folder, chr_names, split_vcf_stats_csv_path, min_mac_range=3, max_mac_range=2, mac_delta=1, min_maf_range=1, max_maf_range=9, maf_delta=1)
