#!/bin/bash

# wrapper_transpose_windows.sh 2 -1 0 0 72

module load bio

mac=$1
maf=$2
window_id=$3
first_index_to_use=$4
expected_number_of_files=$5

# print the inputs
echo "mac: $mac"
echo "maf: $maf"
echo "window_id: $window_id"
echo "first_index_to_use: $first_index_to_use"
echo "expected_number_of_files: $expected_number_of_files"

cmd="python3 /cs/icore/amir.rubin2/code/snpnmi/utils/transpose_windows.py $mac $maf ${window_id} ${first_index_to_use} ${expected_number_of_files}"
echo "$cmd"
#eval "$cmd"
