import sys
import logging
logging.basicConfig()
sys.path.extend(['.', '..'])

import morp

class Foo(morp.Message):
    name = "queue1"

while True:
    with Foo.recv() as f:
        print(f"receiving: {f.fields}")

