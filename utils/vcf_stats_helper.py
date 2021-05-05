import subprocess
import sys
import os
from os.path import dirname, abspath

# params are: input gzvcf_file, output folder, prefix(chr name is good, like chr9)
# example:
# python3 vcf_stats_helper.py /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/hgdp_wgs.20190516.full.chr9.vcf.gz /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/stats/ chr9 freq

StatTypes = ['freq', 'idepth', 'ldepth', 'lqual', 'imiss', 'lmiss']


def validate_stat_types(stat_types):
    for t in stat_types:
        if not t in StatTypes:
            return False
        return True


def get_vcf_stats(gzvcf_folder, gzvcf_file, output_path_prefix, stat_type):
    assert stat_type in StatTypes, f'{stat_type} not one of {",".join(StatTypes)}'

    output_folder = dirname(output_path_prefix)

    print(f'Will extract stats {stat_type} of {gzvcf_file} to path prefixed with: {output_path_prefix}')

    os.makedirs(output_folder, exist_ok=True)

    cmd_parts_base = ['vcftools', '--gzvcf', gzvcf_folder+gzvcf_file, '--max-alleles', '2', '--min-alleles', '2', '--remove-indels', '--max-missing', '0.9']

    # Calculate loci freq
    if stat_type == 'freq':
        freq_cmd = cmd_parts_base + ['--freq', '--out', output_path_prefix + '.freq']
        print('freq_cmd', freq_cmd)
        subprocess.run(freq_cmd)

    # Calculate mean depth per individual
    if stat_type == 'idepth':
        depth_i_cmd = cmd_parts_base + ['--depth', '--out', output_path_prefix + '.idepth']
        print('depth_i_cmd')
        subprocess.run(depth_i_cmd)

    # Calculate mean depth per site
    if stat_type == 'ldepth':
        depth_s_cmd = cmd_parts_base + ['--site-mean-depth', '--out', output_path_prefix + '.ldepth']
        print('depth_s_cmd', depth_s_cmd)
        subprocess.run(depth_s_cmd)

    # Calculate site quality
    if stat_type == 'lqual':
        quality_s_cmd = cmd_parts_base + ['--site-quality', '--out', output_path_prefix + '.lqual']
        print('quality_s_cmd')
        subprocess.run(quality_s_cmd)

    # Calculate proportion of missing data per individual
    if stat_type == 'imiss':
        missing_i_cmd = cmd_parts_base + ['--missing-indv', '--out', output_path_prefix + '.imiss']
        print('missing_i_cmd')
        subprocess.run(missing_i_cmd)

    # Calculate proportion of missing data per site
    if stat_type == 'lmiss':
        missing_s_cmd = cmd_parts_base + ['--missing-site', '--out', output_path_prefix + '.lmiss']
        print('missing_s_cmd', missing_s_cmd)
        subprocess.run(missing_s_cmd)

    return True
