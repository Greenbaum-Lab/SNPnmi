# will use all classes distances matrixes to create a big matrix with all data
# takes about 1 minute to group 66 windows
# python3 3_sum_distances_from_all_classes.py 2 18 1 49

# the commands to use to run netstruct on the result:
# mkdir /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/sanity_check/netstruct/mac_2-18_maf_1-49_windows_0-499/
# sbatch --time=72:00:00 --mem=5G --error="/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/sanity_check_3/netstructh_all_0-499_v3.stderr" --output="/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/sanity_check_3/netstructh_all_0-499_v3.stdout" --job-name="s3_nt_a" /cs/icore/amir.rubin2/code/snpnmi/cluster/wrapper_max_30_params.sh java -jar /cs/icore/amir.rubin2/code/NetStruct_Hierarchy/NetStruct_Hierarchy_v1.1.jar -ss 0.001 -minb 5 -mino 5 -pro /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/sanity_check/netstruct/mac_2-18_maf_1-49_windows_0-499/ -pm /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/sanity_check/distances/mac_2-18_maf_1-49_windows_0-499_norm_dist.tsv.gz -pmn /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/hgdp_wgs.20190516.indlist.csv -pss /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/hgdp_wgs.20190516.SampleSites.txt -w true


import pandas as pd
import json
import os
import gzip
import sys
import time
import sys
from os.path import dirname, abspath
root_path = dirname(dirname(abspath(__file__)))
sys.path.append(root_path)

from utils.common import get_paths_helper
from utils.similarity_helper import generate_similarity_matrix


def sum_all_classes(mac_min_range, mac_max_range, maf_min_range, maf_max_range):
    paths_helper = get_paths_helper()
    dist_dir = paths_helper.dist_folder
    # get inputs
    windows_files = []
    for mac_maf in ['mac', 'maf']:
        is_mac = mac_maf == 'mac'
        min_range = mac_min_range if is_mac else maf_min_range
        max_range = mac_max_range if is_mac else maf_max_range
        if min_range>0:
            print(f'go over {mac_maf} values: [{min_range},{max_range}]')
            for val in range(min_range, max_range+1):
                # in maf we take 0.x
                if not is_mac:
                    val = f'{val * 1.0/100}'
                slice_count_distances_file = f'{dist_dir}{mac_maf}_{val}_all_count_dist.tsv.gz'
                windows_files.append(slice_count_distances_file)

    generate_similarity_matrix(windows_files, dist_dir, f'all_mac_{mac_min_range}-{mac_max_range}_maf_{maf_min_range}-{maf_max_range}', override=False)

def main(args):
    s = time.time()
    print ('Number of arguments:', len(args), 'arguments.')
    print ('Argument List:', str(args))
    # by mac
    mac_min_range = int(args[0])
    mac_max_range = int(args[1])

    # by maf
    maf_min_range = int(args[2])
    maf_max_range = int(args[3])

    # submission details - TODO maybe we can add this to support specific files
    #min_window_index =  int(args[4])
    #max_window_index =  int(args[5])

    # print the inputs
    print('mac_min_range', mac_min_range)
    print('mac_max_range', mac_max_range)
    print('maf_min_range', maf_min_range)
    print('maf_max_range', maf_max_range)

    sum_all_classes(mac_min_range, mac_max_range, maf_min_range, maf_max_range)

    print(f'{(time.time()-s)/60} minutes total run time')

# main([2, 18, 1, 49])

if __name__ == "__main__":
   main(sys.argv[1:])