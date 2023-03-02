# -*- coding: utf-8 -*-

from morp.compat import *
from . import TestCase, testdata


class CLITest(TestCase):
    def test_consume(self):
        c = self.get_client([
            "from morp import Message",
            "",
            "class Consume(Message):",
            "    def target(self):",
            "        print(self.text)"
            "",
            "class Consume2(Consume):",
            "    pass",
        ])

        m = c.message_classes[0].create(text="foobar")
        r = c.recv()
        self.assertTrue(m.text in r)

        m = c.message_classes[1].create(text="bazche")
        r = c.recv()
        self.assertTrue(m.text in r)

