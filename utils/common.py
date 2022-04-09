import argparse
import random
import subprocess
import sys
import time

import pandas as pd
from os.path import dirname, abspath
import os
import gzip
import pyhash
hasher = pyhash.metro_64()



root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)


from utils.config import *
from utils.filelock import FileLock
from utils.paths_helper import PathsHelper


class Cls:
    def __init__(self, mac_maf, int_val):
        self.mac_maf = mac_maf
        self.is_mac = mac_maf == 'mac'
        self.int_val = int_val
        self.val = int_val if self.is_mac else int_val / 100
        self.name = f"{mac_maf}_{self.val}"
        self.max_val = self.val if self.is_mac else (int_val + 1) / 100

def class_iter(options):
    if options.mac[1] >= options.mac[0]:
        for val in range(options.mac[0], options.mac[1] + 1):
            if options.dataset_name == 'arabidopsis' and val % 2 == 1:
                continue
            obj = Cls('mac', val)
            yield obj

    if options.maf[1] >= options.maf[0]:
        for val in range(options.maf[0], options.maf[1] + 1):
            if val > 49:
                continue
            obj = Cls('maf', val)
            yield obj


# a class to represent classes of alleles count/frequency (mac/maf)
class AlleleClass:
    def __init__(self, mac_maf, class_min_int_val, class_max_int_val=None):
        assert mac_maf in ['mac', 'maf']
        self.mac_maf = mac_maf
        self.is_mac = mac_maf == 'mac'

        assert isinstance(class_min_int_val,
                          int), f'The class_min_int_val must be an int, even if its maf - we convert it.'
        assert class_min_int_val >= 0, f'The class_min_int_val must be non-negative.'
        # maf must be lower than 50
        assert self.is_mac | (class_min_int_val < 50)
        # the name of the class value is the int min value, even if its maf
        self.class_int_val_name = class_min_int_val
        # set the max val of the class
        if not class_max_int_val:
            # for mac, we want max_val == min_val [2,2]
            if self.is_mac:
                class_max_int_val = class_min_int_val
            # for maf, we want max_val to be min_val+1 [2, 3) (/100)
            # below we will make sure to exclude maf==0.3 from the interval
            else:
                class_max_int_val = class_min_int_val + 1
        if self.is_mac:
            self.class_min_val = class_min_int_val
            self.class_max_val = class_max_int_val
        # for maf we convert to 0.X
        else:
            self.class_min_val = (class_min_int_val * 1.0 / 100.0)
            self.class_max_val = (class_max_int_val * 1.0 / 100.0)

        self.class_name = f'{mac_maf}_{self.class_min_val}'


def hash_args(options):
    args = options.args + options.mac + options.maf + [options.ns_ss]
    args = [str(e) for e in args]
    return hasher(*args)


def get_paths_helper(dataset_name):
    paths_config = get_config(CONFIG_NAME_PATHS)
    root_data_folder = paths_config['cluster_data_folder'] if is_cluster() else paths_config['local_data_folder']
    root_code_folder = paths_config['cluster_code_folder'] if is_cluster() else paths_config['local_code_folder']

    return PathsHelper(root_data_folder, root_code_folder, dataset_name)


def is_cluster():
    # danger! make sure local code is not under this path!
    return '/sci/labs/' in os.path.abspath(__file__)


def load_dict_from_json(json_path):
    with FileLock(json_path):
        if not os.path.exists(json_path) or os.stat(json_path).st_size == 0:
            with open(json_path, "w+") as f:
                f.write("{}")
            return {}
        with open(json_path, "r") as f:
            data = json.load(f)
        return data


def handle_hash_file(class_name, paths_helper, windows_id_list):
    windows_id_list = [int(wind) for wind in windows_id_list]
    hash_file = paths_helper.hash_windows_list_template.format(class_name=class_name)
    data = load_dict_from_json(hash_file)
    with FileLock(hash_file):
        hash_codes = [int(i) for i in data.keys()]
        new_hash = 0 if len(hash_codes) == 0 else min([i+1 for i in hash_codes if (i+1) not in hash_codes])
        if windows_id_list not in data.values():
            data[str(new_hash)] = windows_id_list
            with open(hash_file, "w") as f:
                json.dump(data, f)
            return new_hash
        else:
            reverse_dict = {tuple(val): key for (key, val) in data.items()}
            return reverse_dict[tuple(windows_id_list)]


def comp_and_save_012_mat(mat, path):
    bin_mat = np.empty(shape=(mat.shape[0], mat.shape[1], 2), dtype=bool)
    bin_mat[:, :, 0] = mat > 0
    bin_mat[:, :, 1] = mat % 2 == 0
    comped_mat = np.packbits(bin_mat)
    np.save(path, comped_mat)


def load_and_decomp_012_mat(path, num_individuals):
    comp_mat = np.load(path)
    unpacked = np.unpackbits(comp_mat).astype(bool).reshape((num_individuals, -1, 2))
    decomp_mat = np.empty(shape=(unpacked.shape[0], unpacked.shape[1]), dtype=np.int8)
    decomp_mat[:, :] = unpacked[:, :, 0] & ~ unpacked[:, :, 1]
    decomp_mat[:, :] -= ~unpacked[:, :, 0] & ~ unpacked[:, :, 1]
    decomp_mat[:, :] += 2 * (unpacked[:, :, 0] & unpacked[:, :, 1])
    return decomp_mat


def write_pairwise_similarity(output_similarity_file, similarity_matrix, output_count_file, count_matrix):
    np.savez_compressed(output_similarity_file, similarity_matrix)
    np.savez_compressed(output_count_file, count_matrix.astype(np.uint32))


def get_number_of_windows_by_class(paths_helper):
    with open(paths_helper.number_of_windows_per_class_path, 'r') as f:
        class2num_windows = json.load(f)
    return class2num_windows


def build_empty_upper_left_matrix(n, default_value):
    return [[default_value] * (n - i) for i in range(1, n)]


def write_upper_left_matrix_to_file(output_file, values):
    with gzip.open(output_file, 'wb') as f:
        for v in values:
            s = ' '.join([str(i) for i in v]) + '\n'
            f.write(s.encode())


def get_window_size(paths_helper):
    with open(paths_helper.windows_dir + 'window_size.txt', 'r') as f:
        window_size = int(f.read())
    return window_size

def str2bool(v) -> bool:
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    if v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    raise Exception('Boolean value expected.')


def get_num_lines_in_file(path, gzip=False):
    if gzip:
        with gzip.open(path, 'rb') as f:
            i = 0
            while f.readline():
                i += 1
            return i
    else:
        with open(path, 'rb') as f:
            i = 0
            while f.readline():
                i += 1
            return i


def get_num_columns_in_file(p, sep='\t', gzip=False):
    if gzip:
        with gzip.open(p, 'rb') as f:
            l = f.readline().decode()
            return len(l.split(sep))
    else:
        with open(p, 'r') as f:
            l = f.readline()
            return len(l.split(sep))


def validate_stderr_empty(err_files):
    for err_file in err_files:
        if not os.path.exists(err_file) or os.stat(err_file).st_size > 0:
            print(f"error in file {err_file}\n\n{err_file}")
            return False
    print("Submitting validation - PASS. All stderr files exists and empty.")
    return True


def warp_how_many_jobs(txt_to_find):

    def how_many_jobs_run(string_to_find=txt_to_find):
        assert is_cluster(), "Cannot check for jobs when run locally"
        username = get_config(CONFIG_NAME_PATHS)["cluster_username"]
        ps = subprocess.Popen(['squeue', '-u', username], stdout=subprocess.PIPE, encoding='utf8')
        try:  # if grep is empty, it raise subprocess.CalledProcessError
            output = subprocess.check_output(('grep', string_to_find), stdin=ps.stdout, encoding='utf8')
            num_of_jobs = output.count("\n")
            return f'({num_of_jobs} running jobs) '
        except subprocess.CalledProcessError:
            return ""

    return how_many_jobs_run


def how_many_local_jobs_run(string_to_find=""):
    top_outputs = os.popen('top -bi -n 1').readlines()
    num_of_running_jobs = len([i for i in top_outputs if string_to_find in i])
    return num_of_running_jobs


# Deprecated?
def get_class2sites(dataset_name):
    path_helper = get_paths_helper(dataset_name)
    split_vcf_output_stats_file = path_helper.split_vcf_stats_csv_path
    df = pd.read_csv(split_vcf_output_stats_file)
    df['mac_or_maf'] = df.apply(lambda r: r['mac'] if r['mac'] != '-' else r['maf'], axis=1)
    class2sites = dict()
    for c in df['mac_or_maf'].unique():
        print('Prepare indexes for class', c)
        all_class_indexes = []
        for i, r in df[df['mac_or_maf'] == c].iterrows():
            chr_n = r['chr_name_name'][3:]
            num_sites = r['num_of_sites_after_filter']
            all_class_indexes = all_class_indexes + [f'{chr_n};{i}' for i in range(num_sites)]
        print('List is ready, size is:', len(all_class_indexes), '. Shuffle the list')
        random.shuffle(all_class_indexes)
        class2sites[c] = all_class_indexes
        print('Done with class', c)
    return class2sites


def args_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--step", dest="step", help="Step number - see README for further info")
    parser.add_argument("-d", "--dataset_name", dest="dataset_name", help="Name of dataset")
    parser.add_argument("--mac", dest="mac", default="2,70", help="min value, max value, delta")
    parser.add_argument("--override", dest="override", action="store_true", help="If true, can override existing files")
    parser.add_argument("--maf", dest="maf", default="1,49", help="min value, max value, delta")
    parser.add_argument("--args", dest="args", help="Any additional args")
    parser.add_argument("--min_max_allele", dest="min_max_allele", default="2,2", )
    parser.add_argument("--ns_ss", dest="ns_ss", default="0.01",
                        help="Net-struct step size (relevant for step 5 only)")
    parser.add_argument("--local_jobs", dest="local_jobs", default=False, action='store_true',
                        help="Net-struct step size (relevant for step 5 only)")
    parser.add_argument("--run_all", dest="run_all", default=False, action='store_true',
                        help="run all pipeline (from step 1.2 till step 5.3) instead of a single step")
    parser.add_argument("--ns_combine", dest="run_ns_together", default=False, action='store_true',
                        help="If use this flag - run NetStruct together per class - submit a single job that will run"
                             "all trees of a certain class one after the other")

    options = parser.parse_args()
    options.args = options.args.split(',') if options.args else []
    options.args = [int(i) if i.isdecimal() else i for i in options.args]
    options.mac = options.mac.split(',') if options.mac else []
    options.mac = [int(i) if i.isdecimal() else i for i in options.mac]
    options.maf = options.maf.split(',') if options.maf else []
    options.maf = [int(i) if i.isdecimal() else i for i in options.maf]
    return options


def str_for_timer(options):
    str = ""
    if options.mac:
        str += f"mac--{options.mac}"
    if options.maf:
        str += f"maf--{options.maf}"
    if options.args:
        str += f"args--{options.args}"
    return str


def repr_num(x):
    if x > 10e4 or x < -10e4:
        return f'{x:.2e}'
    if -1 / 10e4 < x < 1 / 10e4:
        return f'{x:.2e}'
    else:
        num_length = np.log10(np.abs(x))
        max_num_of_digits_after_dot = min(5 - int(num_length), 5)
        return round(x, max_num_of_digits_after_dot)