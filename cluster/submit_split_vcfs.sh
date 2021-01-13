#!/bin/bash

# will submit split_vcfs of given vcfs, using given mac and mafs values
# --error="/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/split_vcfs/stderr/split_vcfs_chr_N_mac_N.stderr"
# --output="/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/split_vcfs/stdout/split_vcfs_chr_N_mac_N.stdout"
# --job-name=split_vcfs_chr_N_mac_N

# submit_split_vcfs.sh "/vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/" "hgdp_wgs.20190516.full.chr1.vcf.gz hgdp_wgs.20190516.full.chr2.vcf.gz hgdp_wgs.20190516.full.chr3.vcf.gz hgdp_wgs.20190516.full.chr4.vcf.gz hgdp_wgs.20190516.full.chr5.vcf.gz hgdp_wgs.20190516.full.chr6.vcf.gz hgdp_wgs.20190516.full.chr7.vcf.gz hgdp_wgs.20190516.full.chr8.vcf.gz hgdp_wgs.20190516.full.chr9.vcf.gz hgdp_wgs.20190516.full.chr10.vcf.gz hgdp_wgs.20190516.full.chr11.vcf.gz hgdp_wgs.20190516.full.chr12.vcf.gz hgdp_wgs.20190516.full.chr13.vcf.gz hgdp_wgs.20190516.full.chr14.vcf.gz hgdp_wgs.20190516.full.chr15.vcf.gz hgdp_wgs.20190516.full.chr16.vcf.gz hgdp_wgs.20190516.full.chr17.vcf.gz hgdp_wgs.20190516.full.chr18.vcf.gz hgdp_wgs.20190516.full.chr19.vcf.gz hgdp_wgs.20190516.full.chr20.vcf.gz hgdp_wgs.20190516.full.chr21.vcf.gz hgdp_wgs.20190516.full.chr22.vcf.gz" "chr1 chr2 chr3 chr4 chr5 chr6 chr7 chr8 chr9 chr10 chr11 chr12 chr13 chr14 chr15 chr16 chr17 chr18 chr19 chr20 chr21 chr22" - - - 0.01 0.49 0.01 "--max-alleles 2 --min-alleles 2 --remove-indels --max-missing 0.9 --012" "/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/split_vcfs/stderr/" "/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/split_vcfs/stdout/"

# submit_split_vcfs.sh "/vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/" "hgdp_wgs.20190516.full.chr22.vcf.gz hgdp_wgs.20190516.full.chr21.vcf.gz" "chr22 chr21" 2 3 1 1 49 1 "--max-alleles 2 --min-alleles 2 --remove-indels --max-missing 0.9 --012" "/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/split_vcfs/stderr/" "/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/split_vcfs/stdout/"
vcfs_folder=$1
vcfs_files=$2
vcf_files_short_names=$3

# by mac
mac_min_range=$4
mac_max_range=$5
mac_delta=$6

# by maf
maf_min_range=$7
maf_max_range=$8
maf_delta=$9

vcftools_params=${10}

# submission details
job_stderr_folder=${11}
job_stdout_folder=${12}
#job_mem
#job_time
#job_comment=

# print the inputs
echo "vcfs_folder: $vcfs_folder"
echo "vcfs_files: $vcfs_files"
echo "vcf_files_short_names: $vcf_files_short_names"

echo "mac_min_range: $mac_min_range"
echo "mac_max_range: $mac_max_range"
echo "mac_delta: $mac_delta"

echo "maf_min_range: $maf_min_range"
echo "maf_max_range: $maf_max_range"
echo "maf_delta: $maf_delta"

echo "vcftools_params: $vcftools_params"

echo "job_stderr_folder: $job_stderr_folder"
echo "job_stdout_folder: $job_stdout_folder"



echo "make job logs folders"
echo "mkdirs - $job_stderr_folder"
mkdir -p ${job_stderr_folder}
echo "mkdirs - $job_stdout_folder"
mkdir -p ${job_stdout_folder}

# split to arrays to validate size
vcfs_files_arr=($vcfs_files)
vcf_files_short_names_arr=($vcf_files_short_names)
if [ ${#vcfs_files_arr[@]} != ${#vcf_files_short_names_arr[@]} ]
then
    echo "vcfs_files and vcf_files_short_names length should be the same"
    exit
fi

echo "Go over the vcf files, submit for each mac and maf a job"
for i in "${!vcfs_files_arr[@]}"; do
    vcf_file=${vcfs_files_arr[i]}
    vcf_short_name=${vcf_files_short_names_arr[i]}
    full_vcf_file_path=${vcfs_folder}${vcf_file}

    output_folder="${vcfs_folder}classes/${vcf_short_name}/"
    echo "process ${vcf_file}, short name: ${vcf_short_name}"

    echo "create output folder - ${output_folder}"
    mkdir -p ${output_folder}

    echo "go over mac values"
    for mac in $(seq $mac_min_range $mac_delta $mac_max_range); do
        job_stderr_file="${job_stderr_folder}${vcf_short_name}_mac${mac}.stderr"
        job_stdout_file="${job_stdout_folder}${vcf_short_name}_mac${mac}.stdout"
        job_name="${vcf_short_name}_mac${mac}"
        cluster_setting='sbatch --error="'${job_stderr_file}'" --output="'${job_stdout_file}'" --job-name="'${job_name}'"'
        cmd_to_run=${cluster_setting}' ../split_vcfs.sh "'${full_vcf_file_path}'" "'${vcftools_params}'" "'${output_folder}'" '$mac' '$mac' 1 - - -'
        echo ${cmd_to_run}
        eval ${cmd_to_run}
    done
    echo "go over maf"
        for maf in $(seq $maf_min_range $maf_delta $maf_max_range); do
        job_stderr_file="${job_stderr_folder}${vcf_short_name}_maf${maf}.stderr"
        job_stdout_file="${job_stdout_folder}${vcf_short_name}_maf${maf}.stdout"
        job_name="${vcf_short_name}_maf${maf}"
        cluster_setting='sbatch --error="'${job_stderr_file}'" --output="'${job_stdout_file}'" --job-name="'${job_name}'"'
	max_maf=$(echo "scale=2;${maf} + ${maf_delta}" | bc)
	cmd_to_run=${cluster_setting}' ../split_vcfs.sh "'${full_vcf_file_path}'" "'${vcftools_params}'" "'${output_folder}'" - - - '$maf' '$max_maf' '$maf_delta
        echo ${cmd_to_run}
        eval ${cmd_to_run}
    done
done
