#!/bin/bash

# will submit calc_distances_in_window of given classes and windows

# submit_calc_dist_windows.sh "/vol/sci/bio/data/gil.greenbaum/amir.rubin/vcf/hgdp/classes/windows/indexes/number_of_windows_per_class.txt" 2 2 1 0.01 0.49 0.01 "/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/calc_dist_windows/stderr/" "/vol/sci/bio/data/gil.greenbaum/amir.rubin/logs/cluster/calc_dist_windows/stdout/"

number_of_windows_per_class_path=$1

# by mac
mac_min_range=$2
mac_max_range=$3
mac_delta=$4

# by maf
maf_min_range=$5
maf_max_range=$6
maf_delta=$7

# submission details
job_stderr_folder=$8
job_stdout_folder=$9


# print the inputs
echo "number_of_windows_per_class_path: $number_of_windows_per_class_path"

echo "mac_min_range: $mac_min_range"
echo "mac_max_range: $mac_max_range"
echo "mac_delta: $mac_delta"

echo "maf_min_range: $maf_min_range"
echo "maf_max_range: $maf_max_range"
echo "maf_delta: $maf_delta"

echo "job_stderr_folder: $job_stderr_folder"
echo "job_stdout_folder: $job_stdout_folder"

echo "make job logs folders"
echo "mkdirs - $job_stderr_folder"
mkdir -p ${job_stderr_folder}
echo "mkdirs - $job_stdout_folder"
mkdir -p ${job_stdout_folder}

echo "go over mac values"
for mac in $(seq $mac_min_range $mac_delta $mac_max_range); do
    # get number of windows:
    a = grep "^${mac} " $number_of_windows_per_class_path
    echo a
    IFS=' ' read -ra ADDR <<< "$a"
    for i in "${ADDR[@]}"; do
        echo $i
    done
    num_windows = ${ADDR[1]}
    echo $num_windows
    window_id=0
    while [ $window_id -lt $(( $num_windows )) ]
    do
        echo $window_id
        window_id=$(($window_id+1))
    done 
    # go over all windows
    # TODO get window index
    job_stderr_file="${job_stderr_folder}mac${mac}_window${window_id}.stderr"
    job_stdout_file="${job_stdout_folder}mac${mac}_window${window_id}.stdout"
    job_name="c${mac}_w${window_id}"
    cluster_setting='sbatch --error="'${job_stderr_file}'" --output="'${job_stdout_file}'" --job-name="'${job_name}'"'
    #maf 0.49 0 0.49 0.5 -1 -1
    cmd_to_run=${cluster_setting}' python3 ../utils/calc_distances_in_window.py mac' $mac' '${window_id}' -1 -1 '$mac' '$mac
    echo ${cmd_to_run}
    #eval ${cmd_to_run}
done
    echo "go over maf"
        for maf in $(seq $maf_min_range $maf_delta $maf_max_range); do
        job_stderr_file="${job_stderr_folder}${vcf_short_name}_maf${maf}.stderr"
        job_stdout_file="${job_stdout_folder}${vcf_short_name}_maf${maf}.stdout"
        job_name="${vcf_short_name}f${maf}"
        cluster_setting='sbatch --error="'${job_stderr_file}'" --output="'${job_stdout_file}'" --job-name="'${job_name}'"'
        max_maf=$(echo "scale=2;${maf} + ${maf_delta}" | bc)
        cmd_to_run=${cluster_setting}' ../split_vcfs.sh "'${full_vcf_file_path}'" "'${vcftools_params}'" "'${output_folder}'" - - '$maf' '$max_maf
        echo ${cmd_to_run}
        #eval ${cmd_to_run}
    done
done
