import subprocess
import sys
import os
from os.path import dirname, abspath

# params are: input gzvcf_file, output folder, prefix(chr name is good, like chr9)
# example:
# python3 vcf_stats_helper.py /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/hgdp_wgs.20190516.full.chr9.vcf.gz /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/stats/ chr9 freq

StatTypes = ['freq', 'idepth', 'ldepth', 'lqual', 'imiss', 'lmiss']
type_flag = {'freq': '--freq', 'idepth': '--depth', 'ldepth': '--site-mean-depth', 'lqual': '--site-mean-depth',
             'imiss': '--missing-indv', 'lmiss': '--missing-site'}
print_names = {'freq': 'freq', 'idepth': 'depth_i', 'ldepth': 'depth_s', 'lqual': 'quality_s', 'imiss': 'missing_i',
               'lmiss': 'missing_s'}

# Make sure we didn't forget any stat
assert all([stat in type_flag.keys() and stat in print_names.keys() for stat in StatTypes])

def validate_stat_types(stat_types):
    for t in stat_types:
        if not t in StatTypes:
            return False
        return True

# TODO - consider submiting to cluster
# TODO 2 - consider support in 'all_stats'
# TODO - possibly can be refactored
def get_vcf_stats(options):
    assert options.stat_type in StatTypes, f'{options.stat_type} not one of {",".join(StatTypes)}'

    output_folder = dirname(options.output_path_prefix)

    print(f'Will extract stats {options.stat_type} of {options.gzvcf_file} to path prefixed with: '
          f'{options.output_path_prefix}')

    os.makedirs(output_folder, exist_ok=True)

    cmd_parts_base = ['vcftools', '--gzvcf', options.vcfs_folder+options.gzvcf_file, '--max-alleles', '2',
                      '--min-alleles', '2', '--remove-indels', '--max-missing', '0.9']

    # Todo: consider the next comment code instead of the following (refactoring):
    # cmd = cmd_parts_base + [type_flag[stat_type], '--out', output_path_prefix + f'.{stat_type}']
    # print(f"{print_names[stat_type]}_cmd")
    # subprocess.run(cmd)

    # Calculate loci freq
    if options.stat_type == 'freq':
        freq_cmd = cmd_parts_base + ['--freq', '--out', options.output_path_prefix + '.freq']
        print('freq_cmd', freq_cmd)
        subprocess.run(freq_cmd)

    # Calculate mean depth per individual
    elif options.stat_type == 'idepth':
        depth_i_cmd = cmd_parts_base + ['--depth', '--out', options.output_path_prefix + '.idepth']
        print('depth_i_cmd')
        subprocess.run(depth_i_cmd)

    # Calculate mean depth per site
    elif options.stat_type == 'ldepth':
        depth_s_cmd = cmd_parts_base + ['--site-mean-depth', '--out', options.output_path_prefix + '.ldepth']
        print('depth_s_cmd', depth_s_cmd)
        subprocess.run(depth_s_cmd)

    # Calculate site quality
    elif options.stat_type == 'lqual':
        quality_s_cmd = cmd_parts_base + ['--site-quality', '--out', options.output_path_prefix + '.lqual']
        print('quality_s_cmd')
        subprocess.run(quality_s_cmd)

    # Calculate proportion of missing data per individual
    elif options.stat_type == 'imiss':
        missing_i_cmd = cmd_parts_base + ['--missing-indv', '--out', options.output_path_prefix + '.imiss']
        print('missing_i_cmd')
        subprocess.run(missing_i_cmd)

    # Calculate proportion of missing data per site
    elif options.stat_type == 'lmiss':
        missing_s_cmd = cmd_parts_base + ['--missing-site', '--out', options.output_path_prefix + '.lmiss']
        print('missing_s_cmd', missing_s_cmd)
        subprocess.run(missing_s_cmd)

    # wrap with try catch?
    return True
