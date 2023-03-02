# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import logging
import sys
import time
import os
import inspect
import subprocess
from collections import defaultdict

from morp.compat import *
from morp import Message
#from morp.interface.sqs import SQS
#from morp.interface import get_interfaces
from morp.exception import ReleaseMessage, AckMessage

from . import TestCase, testdata


class MessageTest(TestCase):
    def test_create(self):
        m = self.get_message(foo=1, bar=2)
        self.assertEqual(1, m.foo)
        self.assertEqual(2, m.bar)

        m2 = Message(
            foo=3,
            bar=4,
        )
        self.assertEqual(3, m2.foo)
        self.assertEqual(4, m2.bar)

    def test_fields(self):
        """just make sure interface_message doesn't end up in the fields dict"""
        m = self.get_message()
        m.imessage = 1
        self.assertFalse("imessage" in m.fields)

    def test_release_1(self):
        m = self.get_message()
        mcls = m.__class__
        m.send()

        with self.assertRaises(RuntimeError):
            with mcls.recv() as m2:
                raise RuntimeError()

        with mcls.recv() as m2:
            self.assertEqual(m2.foo, m.foo)

    def test_release_message(self):
        m = self.get_message()
        mcls = m.__class__
        m.send()

        with mcls.recv() as m2:
            raise ReleaseMessage(2)

        with mcls.recv_for(1) as m2:
            self.assertEqual(None, m2)

        with mcls.recv_for(1) as m2:
            self.assertEqual(m.foo, m2.foo)

    def test_ack_message(self):
        m = self.get_message()
        mcls = m.__class__
        m.send()

        with mcls.recv() as m2:
            raise AckMessage()

        self.assertEqual(0, m.interface.count(name=m.get_name()))

        with mcls.recv_for(timeout=0.1) as m2:
            self.assertEqual(None, m2)

    def test_send_recv(self):
        m = self.get_message()
        m.send()

        with m.__class__.recv() as m2:
            self.assertEqual(m.fields, m2.fields)

    def test_send_later(self):
        m = self.get_message()
        m.send_later(2)

        with m.__class__.recv_for(1) as m2:
            self.assertEqual(None, m2)

        time.sleep(1)

        with m.__class__.recv_for(1) as m2:
            self.assertEqual(m.fields, m2.fields)

    def test_recv_block_success(self):
        m = self.get_message()
        m.send()

        with m.__class__.recv() as m2:
            self.assertEqual(m.fields, m2.fields)

    def test_recv_block_error(self):
        m = self.get_message()
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

        self.assertEqual(0, m.interface.count(name=m.get_name()))

        with mcls.recv_for(timeout=0.1) as m2:
            self.assertEqual(None, m2)

