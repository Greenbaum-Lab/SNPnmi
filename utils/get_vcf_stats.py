import subprocess
import sys
import os
 

def get_vcf_stats():
    gzvcf_file = sys.argv[1]
    output_folder = sys.argv[2]
    print(f'Will extract stats of {gzvcf_file}, output to: {output_folder}')

    os.makedirs(output_folder, exist_ok=True)

    cmd_parts_base = ['vcftools', '--gzvcf', gzvcf_file, '--out', output_folder, '--max-alleles', '2', '--min-alleles', '2']

    print(cmd_parts_base)

    # Calculate mean depth per individual
    depth_i_cmd = cmd_parts_base + ['--depth',]
    print('depth_i_cmd')
    subprocess.run(depth_i_cmd)

    # Calculate mean depth per site
    depth_s_cmd = cmd_parts_base + ['--site-mean-depth',]
    print('depth_s_cmd')
    subprocess.run(depth_s_cmd)

    # Calculate site quality
    quality_s_cmd = cmd_parts_base + ['--site-quality',]
    print('quality_s_cmd')
    subprocess.run(quality_s_cmd)

    # Calculate proportion of missing data per individual
    missing_i_cmd = cmd_parts_base + ['--missing-indv',]
    print('missing_i_cmd')
    subprocess.run(missing_i_cmd)

    # Calculate proportion of missing data per site
    missing_s_cmd = cmd_parts_base + ['--missing-site',]
    print('missing_s_cmd')
    subprocess.run(missing_s_cmd)

if __name__ == '__main__':
    get_vcf_stats()