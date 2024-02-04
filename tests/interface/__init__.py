# -*- coding: utf-8 -*-
from collections import defaultdict

from .. import TestCase, skipIf


class _InterfaceTest(TestCase):
    def test_connection_open_close(self):
        inter = self.get_interface()
        self.assertTrue(inter.connected)

        inter.close()
        self.assertFalse(inter.connected)

    def test_queue_auto_create(self):
        """queues should auto-create, this just makes sure that works as
        intended"""
        m = self.get_message()
        name = m.get_name()
        inter = m.interface

        inter.unsafe_delete(name)

    def test_fields_body_lifecycle(self):
        name = self.get_name()
        inter = self.get_encrypted_interface()
        fields1 = self.get_fields()

        body = inter.fields_to_body(fields1)
        fields2 = inter.body_to_fields(body)
        self.assertEqual(fields1, fields2)

    def test_fields_body_encrypted_lifecycle(self):
        name = self.get_name()
        inter = self.get_encrypted_interface()
        fields1 = self.get_fields()

        body = inter.fields_to_body(fields1)
        fields2 = inter.body_to_fields(body)
        self.assertEqual(fields1, fields2)

    def test_send_count_recv(self):
        msg = self.get_message()
        name = msg.get_name()
        inter = msg.interface

        inter.send(name, msg.fields)

        self.assertEqual(1, inter.count(name))

        fields = inter.recv(name)
        self.assertEqualFields(msg.fields, fields)

        inter.ack(name, fields)
        self.assertEventuallyEqual(0, lambda: inter.count(name))

    def test_recv_timeout(self):
        timeout = 1 # 1s as an int is minimum for SQS
        m = self.get_message()
        with self.assertWithin(1.5):
            m.interface.recv(m.get_name(), timeout=timeout) 

    def test_recv_atomic(self):
        name = self.get_name()
        inter1 = self.get_interface()
        inter2 = self.get_interface()

        inter1.send(name, self.get_fields())

        m2 = inter1.recv(name)
        m3 = inter2.recv(name)
        self.assertIsNotNone(m2)
        self.assertIsNone(m3)

    def test_send_recv_encrypted(self):
        m1 = self.get_message(interface=self.get_encrypted_interface())
        name = m1.get_name()
        m1.interface.send(name, m1.fields)

        fields = m1.interface.recv(name)
        self.assertEqualFields(m1.fields, fields)

        m1.interface.ack(name, fields)

    def test_ack_message(self):
        m1 = self.get_message()
        m1.send()

        with type(m1).recv() as m2:
            m2.ack()

        inter = m2.interface
        name = m2.get_name()
        self.assertEventuallyEqual(0, lambda: inter.count(name))

    def test_release_interface(self):
        m = self.get_message()
        name = m.get_name()
        inter = m.interface
        inter.send(name, m.fields)

        fields = inter.recv(name)
        self.assertEqual(1, fields["_count"])
        inter.release(name, fields)

        fields = inter.recv(name)
        self.assertFalse(fields)
        self.assertEventuallyEqual(1, lambda: inter.count(name))

    def test_release_message(self):
        m = self.get_message()
        m.send()

        with type(m).recv() as m2:
            self.assertEqual(1, m2._count)
            m2.release()

