#!/bin/bash

module load bio

cmd2run=$1

#echo "$cmd2run"
eval "$cmd2run"
