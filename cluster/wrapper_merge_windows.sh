#!/bin/bash

# wrapper_merge_windows.sh maf 0.49 1000 -1 False

module load bio

mac_or_maf=$1
class_name=$2
num_windows_per_slice=$3
num_slices=$4
is_random=$5

# print the inputs
echo "mac_or_maf: $mac_or_maf"
echo "class_name: $class_name"
echo "num_windows_per_slice: $num_windows_per_slice"
echo "num_slices: $num_slices"
echo "is_random: $is_random"

cmd="python3 /cs/icore/amir.rubin2/code/snpnmi/utils/merge_windows_to_slices.py $mac_or_maf $class_name ${num_windows_per_slice} ${num_slices} ${is_random}"
echo "$cmd"
eval "$cmd"
