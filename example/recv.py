import sys
import logging
logging.basicConfig()
sys.path.extend(['.', '..'])

import morp

class Foo(morp.Message):
    pass

while True:
    with Foo.recv() as f:
        print "receiving: {}".format(f.fields)

