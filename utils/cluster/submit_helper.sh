#!/bin/bash
module load vcftools
module load rclone
cmd2run=$1

echo "$cmd2run"
eval "$cmd2run"
