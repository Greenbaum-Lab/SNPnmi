#!/bin/bash

# a wrapper around calc_distances_in_window.py to submit via cluster

# wrapper_calc_dist_windows.sh maf 0.49 630 0.49 0.5 -1 -1

module load bio

mac_or_maf=$1
class_name=$2
min_window_id=$3
max_window_id=$4
maf=$5
max_maf=$6
mac=$7
max_mac=$8
use_specific_012_file=$9
input_012_file_index=${10}

# print the inputs
echo "mac_or_maf: $mac_or_maf"
echo "class_name: $class_name"
echo "min_window_id: $min_window_id"
echo "max_window_id: $max_window_id"
echo "maf: $maf"
echo "max_maf: $max_maf"
echo "mac: $mac"
echo "max_mac: $max_mac"
echo "use_specific_012_file: $use_specific_012_file"
echo "input_012_file_index: $input_012_file_index"

cmd="python3 /cs/icore/amir.rubin2/code/snpnmi/utils/calc_distances_in_window.py $mac_or_maf $class_name ${min_window_id} ${max_window_id} ${maf} ${max_maf} ${mac} ${max_mac} ${use_specific_012_file} ${input_012_file_index}"
echo "$cmd"
eval "$cmd"
