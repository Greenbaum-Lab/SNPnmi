#!/bin/bash
module load bcftools
module load rclone
cmd2run=$1
which python
eval "$cmd2run"
