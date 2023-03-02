# -*- coding: utf-8 -*-
from collections import defaultdict

from .. import TestCase, Client, testdata


class BaseTestInterface(TestCase):
    def test_queue_auto_create(self):
        """queues should auto-create, this just makes sure that works as intended"""
        m = self.get_message()
        name = m.get_name()
        inter = m.interface

        inter.unsafe_delete(name)

    def test_message_lifecycle(self):
        inter = self.get_encrypted_interface()
        fields = {"foo": 1, "bar": 2}
        im = inter.create_imessage(name="message-lifecycle", fields=fields)
        body = im.to_interface()

        im2 = inter.create_imessage(name="message-lifecycle", body=body)
        self.assertEqual(im.fields, im2.fields)
        self.assertEqual(im.fields, fields)

    def test_message_encode_decode(self):
        fields = {"foo": testdata.get_words(), "bar": testdata.get_int()}
        inter = self.get_encrypted_interface()

        im = inter.create_imessage(name="message-lifecycle", fields=fields)
        cipher_text = im.to_interface()
        im2 = inter.create_imessage(name="message-lifecycle", body=cipher_text)
        self.assertEqual(fields, im2.fields)

    def test_send_count_recv(self):
        msg = self.get_message()
        name = msg.get_name()
        inter = msg.interface

        inter.send(name, msg.fields)

        self.assertEqual(1, inter.count(name))

        # re-connect to receive the message
        imsg2 = inter.recv(name)
        self.assertEqual(msg.fields, imsg2.fields)

        inter.ack(name, imsg2)
        self.assertEventuallyEqual(0, lambda: inter.count(name))

    def test_recv_timeout(self):
        m = self.get_message()
        with self.assertWithin(1.5):
            m.interface.recv(m.get_name(), timeout=1)

    def test_send_recv_encrypted(self):
        m1 = self.get_message(interface=self.get_encrypted_interface())
        name = m1.get_name()
        m1.send()

        im2 = m1.interface.recv(name)
        self.assertEqual(m1.fields, im2.fields)
        m1.interface.ack(name, im2)

    def test_release(self):
        m = self.get_message()
        name = m.get_name()
        inter = m.interface
        inter.send(name, m.fields)

        im = inter.recv(name)
        self.assertEqual(1, im._count)
        inter.release(name, im)
        self.assertEqual(1, inter.count(name))

        im = inter.recv(name)
        self.assertIsNone(im)
        self.assertEqual(1, inter.count(name))

    def test_backoff(self):
        m = self.get_message(
            config=self.get_config(backoff_multiplier=1, backoff_amplifier=1),
            foo=testdata.get_int()
        )
        mcls = m.__class__
        m.send()

        count = 0
        for x in range(2):
            with self.assertRaises(RuntimeError):
                with mcls.recv() as m2:
                    self.assertGreater(m2.imessage._count, count)
                    count = m2.imessage._count
                    raise RuntimeError()

        with mcls.recv() as m2:
            self.assertGreater(m2.imessage._count, count)
            self.assertEqual(m2.foo, m.foo)

