#!/bin/bash

module load bio

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

cmd="python3 ${p1} ${p2} ${p3} ${p4} ${p5} ${p6} ${p7} ${p8} ${p9} ${p10} ${p11}"
echo "$cmd"
eval "$cmd"
