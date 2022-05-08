#!/bin/bash
module load vcftools
module load rclone
cmd2run=$1

eval "$cmd2run"
