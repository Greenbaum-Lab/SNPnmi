

# given a dataset name and step number, will run the step.
import time
import sys
import os
from os.path import dirname, abspath
import ftplib
from pathlib import Path
root_path = dirname(dirname(dirname(os.path.abspath(__file__))))
sys.path.append(root_path)
from steps.s1_get_data import get_data


def run_step(dataset_name, step):
    if step == "1":
        return get_data.main([dataset_name])


def runner(args):
    s = time.time()
    print ('Number of arguments:', len(args), 'arguments.')
    print ('Argument List:', str(args))
    dataset_name = args[0]
    step = args[1]

    print(f'Executing step {step} on dataset {dataset_name}.')
    print(run_step(dataset_name, step))

    print(f'{(time.time()-s)/60} minutes total run time')

runner(['hgdp_test','1'])

if __name__ == "__Xmain__":
   runner(sys.argv[1:])
