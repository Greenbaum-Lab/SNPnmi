import subprocess
import sys
import os

# params are: input gzvcf_file, output folder, prefix(chr name is good, like chr9)
# example:
# python3 vcf_stats_helper.py /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/hgdp_wgs.20190516.full.chr9.vcf.gz /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/stats/ chr9 freq

def get_vcf_stats(gzvcf_file, output_folder, chr_name, stat_type):
    output_file_prefix = output_folder + chr_name
    print(f'Will extract stats of {gzvcf_file}, output to: {output_file_prefix}')

    os.makedirs(output_folder, exist_ok=True)

    cmd_parts_base = ['vcftools', '--gzvcf', gzvcf_file, '--max-alleles', '2', '--min-alleles', '2', '--remove-indels', '--max-missing', '0.9']

    print(cmd_parts_base)

    # Calculate loci freq
    if stat_type == 'freq':
        freq_cmd = cmd_parts_base + ['--freq', '--out', output_file_prefix + '.freq']
        print('freq_cmd', freq_cmd)
        subprocess.run(freq_cmd)

    # Calculate mean depth per individual
    if stat_type == 'idepth':
        depth_i_cmd = cmd_parts_base + ['--depth', '--out', output_file_prefix + '.idepth']
        print('depth_i_cmd')
        subprocess.run(depth_i_cmd)

    # Calculate mean depth per site
    if stat_type == 'ldepth':
        depth_s_cmd = cmd_parts_base + ['--site-mean-depth', '--out', output_file_prefix + '.ldepth']
        print('depth_s_cmd', depth_s_cmd)
        subprocess.run(depth_s_cmd)

    # Calculate site quality
    if stat_type == 'lqual':
        quality_s_cmd = cmd_parts_base + ['--site-quality', '--out', output_file_prefix + '.lqual']
        print('quality_s_cmd')
        subprocess.run(quality_s_cmd)

    # Calculate proportion of missing data per individual
    if stat_type == 'imiss':
        missing_i_cmd = cmd_parts_base + ['--missing-indv', '--out', output_file_prefix + '.imiss']
        print('missing_i_cmd')
        subprocess.run(missing_i_cmd)

    # Calculate proportion of missing data per site
    if stat_type == 'lmiss':
        missing_s_cmd = cmd_parts_base + ['--missing-site', '--out', output_file_prefix + '.lmiss']
        print('missing_s_cmd', missing_s_cmd)
        subprocess.run(missing_s_cmd)

if __name__ == '__main__':
    gzvcf_file = sys.argv[1]
    output_folder = sys.argv[2]
    chr_name = sys.argv[3] # like chr9
    get_vcf_stats(gzvcf_file, output_folder, chr_name)
