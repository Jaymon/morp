# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import logging
import sys
import time
import os
from unittest import TestCase
import inspect
import subprocess
from collections import defaultdict

import testdata

#import morp
from morp.compat import *
from morp import Message, Connection, DsnConnection
from morp.interface.sqs import SQS
from morp.interface import get_interfaces
from morp.exception import ReleaseMessage, AckMessage


# configure root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
log_handler = logging.StreamHandler(stream=sys.stderr)
log_formatter = logging.Formatter('[%(levelname).1s] %(message)s')
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)

logger = logging.getLogger('boto3')
logger.setLevel(logging.WARNING)
logger = logging.getLogger('botocore')
logger.setLevel(logging.WARNING)


class MessageTest(BaseInterfaceTestCase):
    interface_class = SQS
#     def get_name(self):
#         #return super(MessageTest, self).get_name('morp-test-message')
#         return 'morp-test-message'

    def get_msg(self, *fields, **fields_kwargs):
        m = self.create_message()
        n = m.name
        i = m.interface
        class TMsg(Message):
            interface = i
            @classmethod
            def get_name(cls): return n

        m = TMsg(*fields, **fields_kwargs)
        return m

    def test_create(self):
        m = self.get_msg(foo=1, bar=2)
        self.assertEqual(1, m.foo)
        self.assertEqual(2, m.bar)

        m2 = Message(
            foo=3,
            bar=4,
            morp_classpath="{}.{}".format(Message.__module__, Message.__name__)
        )
        self.assertEqual(3, m2.foo)
        self.assertEqual(4, m2.bar)

    def test_fields(self):
        """just make sure interface_message doesn't end up in the fields dict"""
        m = self.get_msg()
        m.interface_message = 1
        self.assertFalse("interface_message" in m.fields)

    def test_backoff(self):
        # TODO make this work with a backoff, this test works but doesn't do any
        # sort of visibility backoff
        m = self.get_msg()
        mcls = m.__class__
        foo = testdata.get_int()
        m.foo = foo

        m.send()

        count = 0
        for x in range(2):
            with self.assertRaises(RuntimeError):
                with mcls.recv() as m2:
                    self.assertGreater(m2.interface_message._count, count)
                    count = m2.interface_message._count
                    raise RuntimeError()

        with mcls.recv() as m2:
            self.assertGreater(m2.interface_message._count, count)
            self.assertEqual(m2.foo, m.foo)

    def test_release_1(self):
        m = self.get_msg(foo=testdata.get_int())
        mcls = m.__class__
        m.send()

        with self.assertRaises(RuntimeError):
            with mcls.recv() as m2:
                raise RuntimeError()

        with mcls.recv() as m2:
            self.assertEqual(m2.foo, m.foo)

    def test_release_message(self):
        m = self.get_msg(foo=testdata.get_int())
        mcls = m.__class__
        m.send()

        with mcls.recv() as m2:
            raise ReleaseMessage(2)

        with mcls.recv_for(1) as m2:
            self.assertEqual(None, m2)

        time.sleep(1)

        with mcls.recv_for(1) as m2:
            self.assertEqual(m.foo, m2.foo)

    def test_ack_message(self):
        m = self.get_msg(foo=testdata.get_int())
        mcls = m.__class__
        m.send()

        with mcls.recv() as m2:
            raise AckMessage()

        with mcls.recv_for(timeout=1) as m2:
            self.assertEqual(None, m2)

    def test_send_recv(self):
        m = self.get_msg(foo=1, bar=2)
        m.send()

        with m.__class__.recv() as m2:
            self.assertEqual(m.fields, m2.fields)

    def test_send_later(self):
        m = self.get_msg(foo=1, bar=2)
        m.send_later(2)

        with m.__class__.recv_for(1) as m2:
            self.assertEqual(None, m2)

        time.sleep(1)

        with m.__class__.recv_for(1) as m2:
            self.assertEqual(m.fields, m2.fields)

    def test_recv_block_success(self):
        m = self.get_msg(foo=10, bar=20)
        m.send()

        with m.__class__.recv() as m2:
            self.assertEqual(m.fields, m2.fields)

    def test_recv_block_error(self):
        m = self.get_msg(foo=10)
        mcls = m.__class__
        m.send()

        kwargs = {
            "vtimeout": 1,
            "timeout": 2
        }

        with self.assertRaises(RuntimeError):
            with mcls.recv(**kwargs) as m2:
                raise RuntimeError()

        time.sleep(1.2)

        kwargs["ack_on_recv"] = True
        with self.assertRaises(RuntimeError):
            with mcls.recv(**kwargs) as m2:
                raise RuntimeError()

        time.sleep(1.2)
        with mcls.recv_for(timeout=1) as m2:
            self.assertEqual(None, m2)



class CLITest(TestCase):
    def test_consume(self):
        c = Client([
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


# so test runner won't try and run it
del BaseInterfaceTestCase

