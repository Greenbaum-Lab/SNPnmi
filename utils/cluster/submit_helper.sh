#!/bin/bash

ulimit -n 1500
cmd2run=$1

echo "$cmd2run"
eval "$cmd2run"
