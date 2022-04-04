#!/usr/bin/python3

# utils/scripts/freq_to_sfs.py -d hgdp
import json
from os.path import dirname, abspath, basename
import sys



root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)


from utils.common import get_paths_helper, args_parser, class_iter
from utils.loader import Timer, Loader


def freq2sfs(options):
    paths_helper = get_paths_helper(options.dataset_name)
    stats_dir = paths_helper.vcf_stats_folder
    file_name = f'{options.dataset_name}.vcf.freq.frq'
    macs = {i:0 for i in range(options.mac[0], options.mac[1] + 1)}
    mafs = {i:0 for i in range(options.maf[0], options.maf[1] + 2)}
    line_num = 0
    with open(stats_dir + file_name, 'r') as f:
        line = f.readline()  # Throw headers
        line = f.readline()
        while line:
            line_num += 1
            if line_num % 1000 == 0:
                print(f"Line number: {line_num}")
            line_lst = line.split()
            freq = line_lst[-1].split(sep=":")[-1]
            freq = float(freq)
            assert 0.5 >= freq > 0
            num_of_chrs = int(line_lst[3])
            mac = freq * num_of_chrs
            assert int(mac) == mac
            if freq < 0.01:
                macs[mac] += 1
            else:
                mafs[int(freq*100)] += 1
            line = f.readline()
    num_of_snps = sum([v for k, v in mafs.items()]) + sum([v for k, v in macs.items()])
    assert num_of_snps == line_num
    with open(f"{stats_dir}/mafs_macs_dicts.json", 'w') as output_file:
        json.dump(([mafs, macs]), output_file)

def main(options):
    with Timer(f"Freq to SFS"):
        with Loader("Freq to SFS"):
            freq2sfs(options)


if __name__ == "__main__":
    arguments = args_parser()
    main(arguments)
