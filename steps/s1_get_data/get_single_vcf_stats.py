import subprocess
import sys
import os
from os.path import dirname, abspath


root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)

from utils.checkpoint_helper import execute_with_checkpoint
from utils.common import args_parser, get_paths_helper

SCRIPT_NAME = os.path.basename(__file__)
StatTypes = ['freq', 'idepth', 'ldepth', 'lqual', 'imiss', 'lmiss']
type_flag = {'freq': '--freq', 'idepth': '--depth', 'ldepth': '--site-mean-depth', 'lqual': '--site-mean-depth',
             'imiss': '--missing-indv', 'lmiss': '--missing-site'}


def validate_stat_types(stat_types):
    for t in stat_types:
        if not t in StatTypes:
            return False
        return True


# TODO 2 - consider support in 'all_stats'
# TODO - possibly can be refactored

def get_vcf_stats(options):
    gzvcf_file = options.args[0]
    stats_type = options.args[1]
    output_path_prefix = options.args[2]
    paths_helper = get_paths_helper(options.dataset_name)
    assert stats_type in StatTypes, f'{stat_type} not one of {",".join(StatTypes)}'

    output_folder = dirname(output_path_prefix)

    print(f'Will extract stats {stat_type} of {gzvcf_file} to path prefixed with: '
          f'{output_path_prefix}')

    os.makedirs(output_folder, exist_ok=True)

    cmd_parts_base = ['vcftools', '--gzvcf', paths_helper.data_dir + options.gzvcf_file, '--max-alleles', '2',
                      '--min-alleles', '2', '--remove-indels', '--max-missing', '0.9']


    # Calculate loci freq
    if options.stat_type == 'freq':
        freq_cmd = cmd_parts_base + ['--freq', '--out', output_path_prefix + '.freq']
        print('freq_cmd', freq_cmd)
        subprocess.run(freq_cmd)

    # Calculate mean depth per individual
    elif options.stat_type == 'idepth':
        depth_i_cmd = cmd_parts_base + ['--depth', '--out', output_path_prefix + '.idepth']
        print('depth_i_cmd')
        subprocess.run(depth_i_cmd)

    # Calculate mean depth per site
    elif options.stat_type == 'ldepth':
        depth_s_cmd = cmd_parts_base + ['--site-mean-depth', '--out', output_path_prefix + '.ldepth']
        print('depth_s_cmd', depth_s_cmd)
        subprocess.run(depth_s_cmd)

    # Calculate site quality
    elif options.stat_type == 'lqual':
        quality_s_cmd = cmd_parts_base + ['--site-quality', '--out', output_path_prefix + '.lqual']
        print('quality_s_cmd')
        subprocess.run(quality_s_cmd)

    # Calculate proportion of missing data per individual
    elif options.stat_type == 'imiss':
        missing_i_cmd = cmd_parts_base + ['--missing-indv', '--out', output_path_prefix + '.imiss']
        print('missing_i_cmd')
        subprocess.run(missing_i_cmd)

    # Calculate proportion of missing data per site
    elif options.stat_type == 'lmiss':
        missing_s_cmd = cmd_parts_base + ['--missing-site', '--out', output_path_prefix + '.lmiss']
        print('missing_s_cmd', missing_s_cmd)
        subprocess.run(missing_s_cmd)

    # wrap with try catch?
    return True


if __name__ == '__main__':
    arguments = args_parser()
    gzvcf_file = arguments.args[0]
    stat_type = arguments.args[1]
    is_executed, msg = execute_with_checkpoint(get_vcf_stats, f'{SCRIPT_NAME}_{gzvcf_file}_{stat_type}', arguments)
    if is_executed:
        print(f'done - {gzvcf_file} - {stat_type}')
