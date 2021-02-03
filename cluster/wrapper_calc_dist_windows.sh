#!/bin/bash

# a wrapper around calc_distances_in_window.py to submit via cluster

# wrapper_calc_dist_windows.sh maf 0.49 630 0.49 0.5 -1 -1

module load bio

mac_or_maf=$1
class_name=$2
window_id=$3
maf=$4
max_maf=$5
mac=$6
max_mac=$7

# print the inputs
echo "mac_or_maf: $mac_or_maf"
echo "class_name: $class_name"
echo "window_id: $window_id"
echo "maf: $maf"
echo "max_maf: $max_maf"
echo "mac: $mac"
echo "max_mac: $max_mac"

cmd="python3 /cs/icore/amir.rubin2/code/snpnmi/utils/calc_distances_in_window.py $mac_or_maf $class_name ${window_id} ${maf} ${max_maf} ${mac} ${max_mac}"
echo "$cmd"
eval "$cmd"
