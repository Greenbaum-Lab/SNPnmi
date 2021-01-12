#!/bin/bash
#SBATCH --mem=4G
#SBATCH --time=24:0:0
#SBATCH --comment=="Split vcf file using mac and/or maf values"
# Will split the given vcf.gz file according to the mac (minor allele count) and maf (minor allele freq)

# Example:
# sbatch split_vcfs.sh "/vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/hgdp_wgs.20190516.full.chr22.vcf.gz" "--max-alleles 2 --min-alleles 2 --remove-indels --max-missing 0.9 --012" "/vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/chr22/" "2 3 4 .. 18" "1 2 3 .. 49" 1
# sbatch split_vcfs.sh "/vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/hgdp_wgs.20190516.full.chr22.vcf.gz" "--max-alleles 2 --min-alleles 2 --remove-indels --max-missing 0.9 --012" "/vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/chr22/" 2 2 1 - - -

module load bio

# params
vcffile=$1
# example vcftools_params: "--max-alleles 2 --min-alleles 2 --remove-indels --max-missing 0.9 --012"
vcftools_params=$2
output_folder=$3
# by mac
mac_min_range=$4
mac_max_range=$5
mac_delta=$6
# by maf
maf_min_range=$7
maf_max_range=$8
maf_delta=$9


echo "vcffile: $vcffile"
echo "vcftools_params: $vcftools_params"
echo "output_folder: $output_folder"

echo "mac_min_range: $mac_min_range"
echo "mac_max_range: $mac_max_range"
echo "mac_delta: $mac_delta"

echo "maf_min_range: $maf_min_range"
echo "maf_max_range: $maf_max_range"
echo "maf_delta: $maf_delta"

for mac in $(seq $mac_min_range $mac_delta $mac_max_range)
do
    # we only perform this if the (log) file does not exist
    output_file="${output_folder}mac_$mac.log"
    if [ -f "$output_file" ]; then
        echo "$output_file exists."
    else 
        echo "Will execute vcftools - $output_file does not exist."
        # example cmd: vcftools --gzvcf  --mac 2 --max-mac 2 --max-alleles 2 --min-alleles 2 --remove-indels --max-missing 0.9 --recode --out /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/chr22/mac_2
        # we create a folder for temp output
        mkdir $output_folder"temp_mac_"$mac
        # as mac are integers, we take the range to be [mac, mac + delta ** - 1 **]
        # otherwise, for delta=1, we would have classes of mac in [2,3] (the --mac and --max-mac are inclusive to the value)

        vcfcmd='vcftools '$vcftools_params' --mac '$mac' --max-mac '$(($mac+$mac_delta-1))' --gzvcf "'$vcffile'" --out "'${output_folder}'mac_'$mac'" --temp "'${output_folder}'temp_mac_'$mac
        echo "$vcfcmd"
        #eval "$vcfcmd"
    fi
done

for maf in $(seq $maf_min_range $maf_delta $maf_max_range)
do
    # we only perform this if the file does not exist
    output_file="${output_folder}maf_$maf.log"
    if [ -f "$output_file" ]; then
        echo "$output_file exists."
    else 
        echo "Will execute vcftools - $output_file does not exist."
        # example cmd: vcftools --gzvcf  --maf 2 --max-maf 3 --max-alleles 2 --min-alleles 2 --remove-indels --max-missing 0.9 --recode --out /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/chr22/maf_2
        # we create a folder for temp output
        mkdir $output_folder"temp_maf_"$maf
        vcfcmd='vcftools '$vcftools_params' --maf '$maf' --max-maf '$(($maf+$maf_delta))' --gzvcf "'$vcffile'" --out "'${output_folder}'maf_'$maf'" --temp "'${output_folder}'temp_maf_'$maf
        echo "$vcfcmd"
        #eval "$vcfcmd"
    fi
done
