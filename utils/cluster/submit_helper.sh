#!/bin/bash
module load bcftools
module load rclone
cmd2run=$1
eval "$cmd2run"