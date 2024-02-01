import sys
import logging

logging.basicConfig(
    format="[%(levelname).1s] %(message)s",
    level=logging.DEBUG,
    stream=sys.stdout
)

sys.path.extend(['.', '..'])

import morp

class Foo(morp.Message):
    name = "queue1"

while True:
    with Foo.recv() as f:
        print(f"receiving: {f.fields}")

