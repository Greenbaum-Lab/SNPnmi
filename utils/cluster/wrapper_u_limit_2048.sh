#!/bin/bash

p1=$1
p2=$2
p3=$3
p4=$4
p5=$5
p6=$6
p7=$7
p8=$8
p9=$9
p10=${10}
p11=${11}
p12=${12}
p13=${13}
p14=${14}
p15=${15}
p16=${16}
p17=${17}
p18=${18}
p19=${19}
p20=${20}
p21=${21}
p22=${22}
p23=${23}
p24=${24}
p25=${25}
p26=${26}
p27=${27}
p28=${28}
p29=${29}
p30=${30}
module load vcftools
ulimit -n 2048
source /sci/labs/gilig/shahar.mazie/icore-data/snpnmi_venv/bin/activate.csh
cmd="${p1} ${p2} ${p3} ${p4} ${p5} ${p6} ${p7} ${p8} ${p9} ${p10} ${p11} ${p12} ${p13} ${p14} ${p15} ${p16} ${p17} ${p18} ${p19} ${p20} ${p21} ${p22} ${p23} ${p24} ${p25} ${p26} ${p27} ${p28} ${p29} ${p30}"
eval "$cmd"