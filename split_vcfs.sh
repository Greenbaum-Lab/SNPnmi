TODO validate and add descrption
#!/bin/bash
#SBATCH --mem=4G
#SBATCH --time=24:0:0

# To run this use:
# sbatch split_vcfs.sh "/vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/hgdp_wgs.20190516.full.chr22.vcf.gz" "--max-alleles 2 --min-alleles 2 --remove-indels --max-missing 0.9 --recode" "/vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/chr22/" "2 3 4 .. 18" "1 2 3 .. 49" 1

# params
vcffile=$1
# example vcftools_params: "--max-alleles 2 --min-alleles 2 --remove-indels --max-missing 0.9 --recode"
vcftools_params=$2
output_folder=$3
mac_values=$4
maf_values=$5
maf_delta=$6

for mac in $mac_values
do
    # example cmd: vcftools --gzvcf  --mac 2 --max-mac 2 --max-alleles 2 --min-alleles 2 --remove-indels --max-missing 0.9 --recode --out /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/chr22/mac_2
    vcfcmd="vcftools "$vcftools_params" --mac "$mac" --max-mac "$mac" --gzvcf "$vcffile" --out "$output_folder"mac_"$mac
    echo "$vcfcmd"
    # eval "$vcfcmd"
done

for mac in $mac_values
do
    # example cmd: vcftools --gzvcf  --mac 2 --max-mac 2 --max-alleles 2 --min-alleles 2 --remove-indels --max-missing 0.9 --recode --out /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/chr22/mac_2
    vcfcmd="vcftools "$vcftools_params" --maf "$maf" --max-maf "$(($maf+$maf_delta))" --gzvcf "$vcffile" --out "$output_folder"maf_"$maf
    echo "$vcfcmd"
    # eval "$vcfcmd"
done
