import json

import numpy as np

with open("mat.json", 'w') as f:
    json.dump({}, f)
with open("mat1.json", "w+") as f:
    json.dump({}, f)
print("done")