import sys
import random
import logging
import sys
import logging


logging.basicConfig(format="[%(levelname).1s] %(message)s", level=logging.DEBUG, stream=sys.stdout)
logger = logging.getLogger('boto3')
logger.setLevel(logging.WARNING)
logger = logging.getLogger('botocore')
logger.setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

sys.path.extend(['.', '..'])

import morp


class Foo(morp.Message):
    name = "example-foo"
    def target(self):
        print("bar: {}, che {}".format(self.bar, self.che))


f = Foo()
f.morp_classpath = "send.Foo"
f.bar = random.randint(0, 500)
f.che = random.randint(0, 500)
print "sending: {}".format(f.fields)
f.send()


# to consume messages:
#   $ python -m morp --quiet --directory=/path/to/morp/example example-foo

