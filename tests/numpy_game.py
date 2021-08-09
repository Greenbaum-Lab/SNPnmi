import json

import numpy as np

from utils.common import get_paths_helper

with open("mat.json", 'w') as f:
    json.dump({}, f)
class_str = 'mac_8'
dataset = 'hgdp_test'
paths_helper = get_paths_helper(dataset)
with open(paths_helper.hash_windows_list_template.format(class_name=class_str), "w") as f:
    json.dump({}, f)
print("done")