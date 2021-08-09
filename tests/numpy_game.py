import json
import os

from utils.filelock import FileLock

with FileLock("mat.json"):
    with open("mat.json", 'w') as f:
        json.dump({}, f)

    with open("mat.json", 'r') as f:
        data = json.load(f)
    print(data)


print("done")