from utils.vcf_stats_helper import get_vcf_stats
import sys

# params are: input_schema, output_folder, comma separated chr_names
# example:
# python get_vcfs_stats.py /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/hgdp_wgs.20190516.full.{chr_name}.vcf.gz /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/stats/ chr1,chr2,chr3,chr4,chr5,chr6,chr7,chr8,chr9,chr10,chr11,chr12,chr13,chr14,chr15,chr16,chr17,chr18,chr19,chr20,chr21,chr22,chrx,chry

def get_vcfs_stats():
    input_schema = sys.argv[1]
    assert 'chr_name' in input_schema
    output_folder = sys.argv[2]
    chr_names = sys.argv[3]
    for chr_name in chr_names.split(','):
        gzvcf_file = input_schema.format(chr_name=chr_name)
        get_vcf_stats(gzvcf_file, output_folder, chr_name)
if __name__ == '__main__':
    get_vcfs_stats()