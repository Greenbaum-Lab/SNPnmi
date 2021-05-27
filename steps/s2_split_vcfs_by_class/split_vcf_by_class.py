DEBUG = False
# Given a specific vcf file and class, will extract the relevant sites
import subprocess
import sys
import os
from os import path
from os.path import dirname, abspath
root_path = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(root_path)
from utils.common import get_paths_helper
from utils.config import *

def split_vcf_by_class(mac_maf, class_min_val, vcf_full_path, vcf_file_short_name, output_dir, class_max_val=None):
    # check input
    assert isinstance(class_min_val, int), f'The class_min_val must be an int, even if its maf - we convert it.'

    # check early break if output file already exists
    # if class_max_val is not given, we increse the class_min_val by 1
    is_mac = 'mac' in mac_maf
    if not class_max_val:
        # for mac, we want max_val == min_val [2,2]
        if is_mac:
            class_max_val = class_min_val
        # for maf, we want max_val to be min_val+1 [2, 3) (/100)
        # below we will make sure to exclude maf==0.3 from the interval
        else:
            class_max_val = class_min_val + 1
    # in maf we convert to 0.x
    if not is_mac:
        class_min_val = (class_min_val*1.0/100.0)
        class_max_val = (class_max_val*1.0/100.0)
    
    output_path = f'{output_dir}{mac_maf}_{class_min_val}'

    # early break if the output file already exists
    output_file = f'{output_path}.012'
    if path.exists(output_file):
        print(f'output file already exist. Break. {output_file}')
        return False

    #prepare output and params
    os.makedirs(output_dir, exist_ok=True)
    # example cmd
    # vcftools --mac 2 --max-mac 2 --max-alleles 2 --min-alleles 2 --remove-indels --max-missing 0.9 --recode --gzvcf "'$vcffile'" --out "'${output_folder}'mac_'$mac'" --temp "'${output_folder}'temp_mac_'$mac'"'
    vcftools_cmd_parts_base = ['vcftools', 
                      '--gzvcf', vcf_full_path, 
                      '--max-alleles', '2', 
                      '--min-alleles', '2', 
                      '--remove-indels', 
                      '--max-missing', '0.9']

    min_val_param_name = '--mac' if is_mac else '--maf'
    max_val_param_name = '--max-mac' if is_mac else '--max-maf'

    # we create a tmp file for vcftools to use, so that we wont override files
    # these can possibly be deleted after the run is done
    # note that for maps, we keep in the tmp folder the excatly_maf sites indexes
    output_temp_dir = f'{output_path}_tmp/'
    os.makedirs(output_temp_dir, exist_ok=True)

    # If maf, first identify sites with maf==max_maf, as we wish to exclude them from the interval
    # The closed interval [0.49, 0.5] is allowed.
    if not is_mac and class_max_val != 0.5:
        output_path_exactly_max_maf = f'{output_temp_dir}exactly_{class_max_val}'
        # note the use of class_max_val in both min and max!
        #vcfcmd='vcftools '$vcftools_params' --maf '${max_maf}' --max-maf '${max_maf}' --gzvcf "'$vcffile'" --out "'$output_folder'temp_maf_'$maf'/exactly_'${max_maf}'" --temp "'${output_folder}'temp_maf_'$maf'" --kept-sites'
        vcftools_cmd_exactly_max_maf = vcftools_cmd_parts_base + [
                                        min_val_param_name, str(class_max_val), 
                                        max_val_param_name, str(class_max_val), 
                                        '--out', output_path_exactly_max_maf, 
                                        '--temp', output_temp_dir,
                                        # this will only output sites indexes, which we later use to exclude
                                        '--kept-sites']
        # add the exclusion to the cmd parts
        vcftools_cmd_parts_base = vcftools_cmd_parts_base + ['--exclude-positions', f'{output_path_exactly_max_maf}.kept.sites']

        print('vcftools_cmd_exactly_max_maf', ' '.join(vcftools_cmd_exactly_max_maf))

        if not DEBUG:
            subprocess.run(vcftools_cmd_exactly_max_maf)

    vcftools_cmd = vcftools_cmd_parts_base + [
                            min_val_param_name, str(class_min_val), 
                            max_val_param_name, str(class_max_val), 
                            '--out', output_path, 
                            '--temp', output_temp_dir,
                            '--012']


    print('vcftools_cmd', ' '.join(vcftools_cmd))
    if not DEBUG:
        subprocess.run(vcftools_cmd)
    return True

def _test_me():
    split_vcf_by_class('maf', 48, 'C:/Data/HUJI/vcf/hgdp_wgs.20190516.full.chr21.vcf.gz', 'chr21', r'C:/Data/HUJI/vcf/hgdp_test/classes/')

if DEBUG:
    _test_me()
elif __name__ == '__main__':
    mac_maf = sys.argv[1]
    class_val = int(sys.argv[2])
    vcf_full_path = sys.argv[3]
    vcf_file_short_name = sys.argv[4]
    classes_dir = sys.argv[5]

    # print the inputs
    print('mac_maf', mac_maf)
    print('class_val', class_val)

    print('vcf_full_path', vcf_full_path)
    print('vcf_file_short_name', vcf_file_short_name)

    print('classes_dir', classes_dir)
    output_dir = classes_dir + vcf_file_short_name + '/'
    split_vcf_by_class(mac_maf, class_val, vcf_full_path, vcf_file_short_name, output_dir)
