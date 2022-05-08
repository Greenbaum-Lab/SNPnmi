#!/bin/bash
module load vcftools
module load rclone
source /sci/labs/gilig/shahar.mazie/icore-data/snpnmi_venv/bin/activate.csh
cmd2run=$1
which python
eval "$cmd2run"
