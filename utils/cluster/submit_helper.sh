#!/bin/bash
module load bcftools
module load rclone
cmd2run=$1
which python
echo "$cmd2run"
eval "$cmd2run"