import sys
import random
import logging
logging.basicConfig()
sys.path.extend(['.', '..'])

import morp


class Foo(morp.Message):
    pass

f = Foo()
f.bar = random.randint(0, 500)
f.che = random.randint(0, 500)
print "sending: {}".format(f.fields)
f.send()

