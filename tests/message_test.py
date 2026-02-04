# -*- coding: utf-8 -*-
import time

from morp.compat import *
from morp import Message
from morp.exception import ReleaseMessage, AckMessage

from . import TestCase


class MessageTest(TestCase):
    def test___init__(self):
        m = self.get_message(foo=1, bar=2)
        self.assertEqual(1, m.foo)
        self.assertEqual(2, m.bar)

    def test_fields(self):
        """just make sure non defined public properties don't end up in the
        fields dict"""
        m = self.get_message()
        m.foobar = 2
        self.assertFalse("foobar" in m.fields)

    async def test_release_1(self):
        m = self.get_message()
        mcls = m.__class__
        await m.send()

        with self.assertRaises(RuntimeError):
            async with mcls._recv() as m2:
                raise RuntimeError()

        async with mcls._recv() as m2:
            self.assertEqual(m2.foo, m.foo)

    async def test_release_message(self):
        m = self.get_message()
        mcls = m.__class__
        await m.send()

        async with mcls._recv() as m2:
            raise ReleaseMessage(2)

        async with mcls._recv_for(1) as m2:
            self.assertEqual(None, m2)

        async with mcls._recv_for(1) as m2:
            self.assertEqual(m.foo, m2.foo)

    async def test_ack_message(self):
        m = self.get_message()
        mcls = m.__class__
        await m.send()

        async with mcls._recv() as m2:
            raise AckMessage()

        self.assertEqual(0, await m.interface.count(name=m.get_name()))

        async with mcls._recv_for(timeout=0.1) as m2:
            self.assertEqual(None, m2)

    async def test_send_recv(self):
        m = self.get_message()
        await m.send()

        async with m.__class__._recv() as m2:
            self.assertEqualFields(m.fields, m2.fields)

    async def test_send_later(self):
        m = self.get_message()
        await m.send(delay_seconds=2)

        async with m.__class__._recv_for(1) as m2:
            self.assertEqual(None, m2)

        time.sleep(1)

        async with m.__class__._recv_for(1) as m2:
            self.assertEqualFields(m.fields, m2.fields)

    async def test_recv_block_success(self):
        m = self.get_message()
        await m.send()

        async with m.__class__._recv() as m2:
            self.assertEqualFields(m.fields, m2.fields)

    async def test_recv_block_error(self):
        m = self.get_message()
        mcls = m.__class__
        await m.send()

        kwargs = {
            "vtimeout": 1,
            "timeout": 2
        }

        with self.assertRaises(RuntimeError):
            async with mcls._recv(**kwargs) as m2:
                raise RuntimeError()

        time.sleep(1.2)

        kwargs["ack_on_recv"] = True
        with self.assertRaises(RuntimeError):
            async with mcls._recv(**kwargs) as m2:
                raise RuntimeError()

        self.assertEqual(0, await m.interface.count(name=m.get_name()))

        async with mcls._recv_for(timeout=0.1) as m2:
            self.assertEqual(None, m2)

    async def test_backoff(self):
        m = self.get_message(
            config=self.get_config(backoff_multiplier=1, backoff_amplifier=1),
            foo=self.get_int()
        )
        mcls = m.__class__
        await m.send()

        count = 0
        for x in range(2):
            with self.assertRaises(RuntimeError):
                async with mcls._recv() as m2:
                    self.assertGreater(m2._hydrate_fields["_count"], count)
                    count = m2._hydrate_fields["_count"]
                    raise RuntimeError()

        async with mcls._recv() as m2:
            self.assertGreater(m2._hydrate_fields["_count"], count)
            self.assertEqual(m2.foo, m.foo)

    async def test_schema(self):
        class Foo(Message):
            bar: int
            che: str
            boo: int = 3
            _ignored: str = ""

        class Bar(Foo):
            bam: int

        b = Bar()

        with self.assertRaises(AttributeError):
            b._to_interface()

        b.bar = 1
        b.che = "two"
        b.bam = 4

        fields = b._to_interface()
        for k in ["bar", "che", "boo", "bam"]:
            self.assertTrue(k in fields)

