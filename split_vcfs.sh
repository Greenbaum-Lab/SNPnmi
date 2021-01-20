#!/bin/bash
#SBATCH --mem=4G
#SBATCH --time=24:0:0
#SBATCH --comment=="Split vcf file using mac and/or maf values"
# Will split the given vcf.gz file according to the mac (minor allele count) and maf (minor allele freq)

# Example:
# sbatch split_vcfs.sh "/vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/hgdp_wgs.20190516.full.chr22.vcf.gz" "--max-alleles 2 --min-alleles 2 --remove-indels --max-missing 0.9" "/vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/chr22/" 2 2 0.4 0.41

module load bio

# params
vcffile=$1
# example vcftools_params: "--max-alleles 2 --min-alleles 2 --remove-indels --max-missing 0.9"
vcftools_params=$2
output_folder=$3
# by mac
mac=$4
max_mac=$5
# by maf
maf=$6
max_maf=$7

echo "vcffile: $vcffile"
echo "vcftools_params: $vcftools_params"
echo "output_folder: $output_folder"

echo "mac: $mac"
echo "max_mac: $max_mac"

echo "maf: $maf"
echo "max_maf: $max_maf"

if [ $mac != "-" ]; then
    output_file="${output_folder}mac_$mac.012"
    if [ -f "$output_file" ]; then
        echo "$output_file exists."
    else 
        echo "Will execute vcftools - $output_file does not exist."
        # example cmd: vcftools --gzvcf  --mac 2 --max-mac 2 --max-alleles 2 --min-alleles 2 --remove-indels --max-missing 0.9 --recode --out /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/chr22/mac_2
        # we create a folder for temp output
        mkdir $output_folder"temp_mac_"$mac
        # as mac are integers, we take the range to be [mac, mac + delta ** - 1 **]
        # otherwise, for delta=1, we would have classes of mac in [2,3] (the --mac and --max-mac are inclusive to the value)

        vcfcmd='vcftools '$vcftools_params' --mac '$mac' --max-mac '$max_mac' --gzvcf "'$vcffile'" --out "'${output_folder}'mac_'$mac'" --temp "'${output_folder}'temp_mac_'$mac'" --012'
        echo "$vcfcmd"
        #eval "$vcfcmd"
    fi
fi

# we only perform this if the 012 file does not exist
# TODO - this has a bug, if the maf is 0.4, the file is with '0.40' and we think it doesnt exists..
if [ $maf != "-" ]; then
    output_file="${output_folder}maf_$maf.012"
    if [ -f "$output_file" ]; then
        echo "$output_file exists."
    else 
        echo "Will execute vcftools - $output_file does not exist."
        # example cmd: vcftools --gzvcf  --maf 0.02 --max-maf 0.03 --max-alleles 2 --min-alleles 2 --remove-indels --max-missing 0.9 --recode --out /vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/chr22/maf_2
        # we create a folder for temp output
        mkdir "${output_folder}temp_maf_${maf}"

        # create a placeholder for the max_maf sites
        # we validate this in chr22 which had 1585 fo maf in [0.48-0.49]:
        # in maf==0.49 we have 1
        # runnig exclusion we got 1584, as expected
        # and, using an empty exclusion file, we got 1585, again, as expected
        echo '' > "${output_folder}temp_maf_${maf}/exactly_${max_maf}.kept.sites"
        # we dont filter out mafs==0.5, as this is the last run
        if [ $max_maf != "0.5" ] && [ $max_maf != "0.50" ] && [ $max_maf != ".50" ] && [ $max_maf != "0.5" ]; then
            echo "First identify sites with maf==max_maf, as we wish to exclude them from the analysis"
            vcfcmd='vcftools '$vcftools_params' --maf '${max_maf}' --max-maf '${max_maf}' --gzvcf "'$vcffile'" --out "'$output_folder'temp_maf_'$maf'/exactly_'${max_maf}'" --temp "'${output_folder}'temp_maf_'$maf'" --kept-sites'
            echo "$vcfcmd"
            eval "$vcfcmd"
        fi

        vcfcmd='vcftools '$vcftools_params' --maf '$maf' --max-maf '${max_maf}' --gzvcf "'$vcffile'" --out "'${output_folder}'maf_'$maf'" --temp "'${output_folder}'temp_maf_'$maf'" --exclude-positions "'${output_folder}'temp_maf_'${maf}'/exactly_'${max_maf}'.kept.sites" --012'
        echo "$vcfcmd"
        eval "$vcfcmd"
    fi
fi
