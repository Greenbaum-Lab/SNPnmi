#!/bin/bash


cmd2run=$1

echo "$cmd2run"
module load bioinfo
module load bio
eval "$cmd2run"
