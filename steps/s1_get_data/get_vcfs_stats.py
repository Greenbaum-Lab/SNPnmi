# Given a set of vcfs on local machine, submit vcftools jobs to get stats
from os import path
import urllib.request
import time
import sys
import os
from os.path import dirname, abspath
import ftplib
from pathlib import Path
root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)
from utils.vcf_stats_helper import get_vcf_stats
from utils.checkpoint_helper import *
from utils.common import get_paths_helper
from utils.config import *

# params are: input_schema, output_folder, comma separated chr_names
# example:
# python3 get_vcfs_stats.py /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/hgdp_wgs.20190516.full.\{chr_name\}.vcf.gz 
# /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/stats/ freq chr1,chr2,chr3,chr4,chr5,chr6,chr7,chr8,chr9,chr10,chr11,chr12,chr13,chr14,chr15,chr16,chr17,chr18,chr19,chr20,chr21,chr22,chrX,chrY

def generate_vcfs_stats(dataset_name, stat_types):
    paths_helper = get_paths_helper(dataset_name)
    vcfs_folder = paths_helper.data_folder
    #TODO from here
    files_names = get_dataset_files_names(dataset_name)
    output_folder = paths_helper.vcf_stats_folder

    stats_done = True
    for gzvcf_file in files_names:
        # vcf file exist
        if path.exists(vcfs_folder + gzvcf_file):
            # stats not caclulated yet
            get_vcf_stats(gzvcf_file, output_folder, chr_name, stat_type)
            print(f'done - {chr_name}')
        else:
            print(f'one of the input files is missing: {gzvcf_file}')
            stats_done = False


# using checkpoint
def get_vcfs_stats(args):
    s = time.time()
    print ('Number of arguments:', len(args), 'arguments.')
    print ('Argument List:', str(args))
    dataset_name = args[0]
    chekpoint_file = get_checkpoint_file_path(dataset_name, os.path.basename(__file__))
    if os.path.exists(chekpoint_file):
        checkpoint_time = get_checkpoint_time(chekpoint_file)
        print (f'Checkpoint exists from {checkpoint_time}. ({chekpoint_file})Break.')
        print(f'{(time.time()-s)/60} minutes total run time')
        return 'checkpoint found'

    success = generate_vcfs_stats(dataset_name)
    if success:
        write_checkpoint_file(chekpoint_file)
    print(f'{(time.time()-s)/60} minutes total run time')
    return 'done'

if __name__ == "__main__":
   get_vcfs_stats(sys.argv[1:])
