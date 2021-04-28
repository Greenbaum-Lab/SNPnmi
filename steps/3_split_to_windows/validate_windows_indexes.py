import os
import json
import pandas as pd

def get_windows(path):
    with open(path) as f:
        windows = json.load(f)
    return windows

def get_chr2number_of_indexes(windows, min_chr, max_chr):
    chr2num_of_indexes= dict()
    for i in range(min_chr, max_chr+1):
        chr2num_of_indexes[str(i)] = 0
    for window in windows:
        for site in window:
            parts = site.split(';')
            chr2num_of_indexes[parts[0]]+=1
    return chr2num_of_indexes

def validate_number_of_sites_in_class(class_name, windows_indexes_path_template, min_chr, max_chr, df):
    windows = get_windows(windows_indexes_path_template.format(class_name=class_name))
    chr2num_of_indexes = get_chr2number_of_indexes(windows, min_chr, max_chr)

    for chr_id in range(min_chr, max_chr+1):
        chr_name = f'chr{chr_id}'
        value_from_vcf_split_stats = df[(df['chr_name_name']==chr_name) & (df['mac_or_maf']==class_name)]['num_of_sites_after_filter'].values[0]
        assert value_from_vcf_split_stats == chr2num_of_indexes[str(chr_id)]

def validate_windows_indexes(windows_indexes_files_folder, number_of_windows_per_class_path, split_vcf_stats_path, windows_indexes_path_template, min_mac, max_mac, min_maf, max_maf, min_chr, max_chr):
    # validate per class that we have a windows indexes file
    for mac in range(min_mac, max_mac+1):
        path = windows_indexes_path_template.format(class_name=str(mac),)
        assert os.path.isfile(path), f'{path} is missing'
    for maf in range(min_maf, max_maf+1):
        path = windows_indexes_path_template.format(class_name=str(maf*1.0/100),)
        assert os.path.isfile(path), f'{path} is missing'
    print(f'PASSED - all windows indexes file of relevant macs and mafs exist')

    #validate number of windows per class
    # read the file
    class2num_of_windows = dict()
    with open(number_of_windows_per_class_path) as f:
        for l in f:
            parts = l.split()
            class_name = parts[0]
            num_of_windows = parts[1] 
            class2num_of_windows[class_name] = int(num_of_windows)

    # validate all classes are there
    for mac in range(min_mac, max_mac+1):
        assert str(mac) in class2num_of_windows.keys(), f'{mac} class is missing in {number_of_windows_per_class_path}'
    for maf in range(min_maf, max_maf+1):
        assert str(maf*1.0/100) in class2num_of_windows.keys(), f'{str(maf*1.0/100)} class is missing in {number_of_windows_per_class_path}'
    print(f'PASSED - all relevant macs and mafs exist in the file with number of windows per class')  

    # validate the number of windows listed is indded the number in the windows indexes file
    for mac in range(min_mac, max_mac+1):
        windows_indexes_path = windows_indexes_path_template.format(class_name=str(mac),)
        num_windows = len(get_windows(windows_indexes_path))
        assert num_windows == class2num_of_windows[str(mac)]
    for maf in range(min_maf, max_maf+1):
        windows_indexes_path = windows_indexes_path_template.format(class_name=str(maf*1.0/100),)
        num_windows = len(get_windows(windows_indexes_path))
        assert num_windows == class2num_of_windows[str(maf*1.0/100)]
    print(f'PASSED - number of windows match those found in the windows indexes files for all relevant macs and mafs')

    # validate that all indexes are covered: sum per class per chr and compare to the file holding the split_vcf stats
    df = pd.read_csv(split_vcf_stats_path)
    df['mac_or_maf'] = df.apply(lambda r : r['mac'] if r['mac']!='-' else r['maf'], axis=1)


    for mac in range(min_mac, max_mac+1):
        validate_number_of_sites_in_class(str(mac), windows_indexes_path_template, min_chr, max_chr, df)
    for maf in range(min_maf, max_maf+1):
        validate_number_of_sites_in_class(str(maf*1.0/100), windows_indexes_path_template, min_chr, max_chr, df)
    print(f'PASSED - number of sites match those found in the split_vcf stats file for all relevant macs and mafs and chrs')

# HDGP
#split_vcf_stats_path = r"C:\Data\HUJI\hgdp\classes\split_vcf_output_stats.csv"
#number_of_windows_per_class_path = r"C:\Data\HUJI\hgdp\classes\number_of_windows_per_class.txt"
#windows_indexes_files_folder = r"C:\Data\HUJI\hgdp\classes\windows_indexes/"

split_vcf_stats_path = r'/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/split_vcfs/split_vcf_output_stats.csv'
number_of_windows_per_class_path = r'/vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/windows/indexes/number_of_windows_per_class.txt'
windows_indexes_files_folder = r'/vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/windows/indexes/'

min_mac = 2
max_mac = 18
min_maf = 1
max_maf = 49
min_chr = 1
max_chr = 22
windows_indexes_path_template = windows_indexes_files_folder + "windows_indexes_for_class_{class_name}.json"
validate_windows_indexes(windows_indexes_files_folder, number_of_windows_per_class_path, split_vcf_stats_path, windows_indexes_path_template, min_mac, max_mac, min_maf, max_maf, min_chr, max_chr)