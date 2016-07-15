#from unittest import TestCase
import logging
import sys
import time
import os
from unittest import TestCase

import testdata

#import morp
from morp import Message, Connection, DsnConnection
from morp.interface.sqs import SQS


# configure root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
log_handler = logging.StreamHandler(stream=sys.stderr)
log_formatter = logging.Formatter('[%(levelname)s] %(message)s')
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)

logger = logging.getLogger('boto3')
logger.setLevel(logging.WARNING)


class BaseInterfaceTestCase(TestCase):
    interface_class = None
    def setUp(self):
        i = self.get_interface()
        n = self.get_name()
        i.clear(n)

    def get_interface(self):
        """get a connected interface"""
        config = DsnConnection(os.environ['MORP_DSN_1'])
        i = self.interface_class(config)
        i.connect()
        self.assertTrue(i.connected)
        return i

    def get_encrypted_interface(self):
        """get a connected interface"""
        config = DsnConnection(os.environ['MORP_DSN_1'])

        if testdata.random.randint(0, 1):
            key_path = testdata.create_file("/morp.key", testdata.get_ascii(100))
            config.options['keyfile'] = key_path
        else:
            config.options['key'] = testdata.get_ascii(32)

        i = self.interface_class(config)
        i.connect()
        self.assertTrue(i.connected)
        return i

    def get_name(self):
        #return 'morp-test-' + testdata.get_ascii(12)
        return 'morp-test-sqs'

    def test_msg_lifecycle(self):
        i = self.get_encrypted_interface()
        im = i.create_msg()

        fields = {"foo": 1, "bar": 2}
        im.fields = fields
        body = im.body

        im2 = i.create_msg()
        im2.body = body
        self.assertEqual(im.fields, im2.fields)
        self.assertEqual(im.fields, fields)


class SQSInterfaceTest(BaseInterfaceTestCase):
    interface_class = SQS
    def test_send_count_recv(self):
        i = self.get_interface()
        n = self.get_name()

        d = {'foo': 1, 'bar': 2}
        interface_msg = i.create_msg(fields={'foo': 10, 'bar': 20})
        i.send(n, interface_msg)
        self.assertEqual(1, i.count(n))

        # re-connect to receive the message
        i2 = self.get_interface()
        interface_msg2 = i2.recv(n)
        self.assertEqual(interface_msg.fields, interface_msg2.fields)

        i2.ack(n, interface_msg2)
        time.sleep(2) # 2 seconds made it work consistently
        self.assertEqual(0, i.count(n))

    def test_recv_timeout(self):
        i = self.get_interface()
        n = self.get_name()
        start = time.time()
        i.recv(n, 1)
        stop = time.time()
        self.assertLessEqual(1.0, stop - start)

    def test_send_recv_encrypted(self):
        i = self.get_encrypted_interface()
        n = self.get_name()
        interface_msg = i.create_msg(fields={'foo': 10, 'bar': 20})
        i.send(n, interface_msg)

        interface_msg2 = i.recv(n)
        self.assertEqual(interface_msg.fields, interface_msg2.fields)
        i.ack(n, interface_msg2)


class MessageTest(BaseInterfaceTestCase):
    interface_class = SQS
    def get_name(self):
        return 'morp-test-message'

    def get_msg(self):
        class TMsg(Message):
            interface = self.get_interface()
            @classmethod
            def get_name(cls): return self.get_name()

        m = TMsg()
        return m

    def test_backoff(self):
        # TODO make this work
        m = self.get_msg()
        mcls = m.__class__
        foo = testdata.get_int()
        m.foo = foo

        m.send()

        for x in range(3):
            with self.assertRaises(RuntimeError):
                with mcls.recv_block() as m2:
                    raise RuntimeError()

        with mcls.recv_block() as m2:
            pout.v(m2)
            self.assertEqual(m2.foo, m.foo)


    def test_release(self):
        m = self.get_msg()
        mcls = m.__class__
        m.foo = testdata.get_int()
        m.send()

        with self.assertRaises(RuntimeError):
            with mcls.recv_block() as m2:
                raise RuntimeError()

        with mcls.recv_block() as m2:
            self.assertEqual(m2.foo, m.foo)

    def test_send_recv(self):
        m = self.get_msg()
        m.foo = 1
        m.bar = 2
        m.send()

        with m.__class__.recv() as m2:
            self.assertEqual(m.fields, m2.fields)

    def test_recv_block(self):
        m = self.get_msg()
        m.foo = 10
        m.bar = 20
        m.send()

        with m.__class__.recv_block() as m2:
            self.assertEqual(m.fields, m2.fields)

    def test_recv_block_error(self):
        m = self.get_msg()
        mcls = m.__class__
        m.foo = 10
        m.send()

        kwargs = {
            "vtimeout": 1,
            "timeout": 2
        }

        with self.assertRaises(RuntimeError):
            with mcls.recv_block(**kwargs) as m2:
                raise RuntimeError()

        time.sleep(1.2)

        kwargs["ack_on_recv"] = True
        with self.assertRaises(RuntimeError):
            with mcls.recv_block(**kwargs) as m2:
                raise RuntimeError()

        time.sleep(1.2)
        with mcls.recv(timeout=1) as m2:
            self.assertEqual(None, m2)


class ConnectionTest(TestCase):
    def test_key(self):
        c = Connection()
        self.assertEqual("", c.key)
        self.assertEqual(c.key, c.key)

        key = testdata.get_ascii(100)
        c = Connection(options=dict(key=key))
        self.assertNotEqual("", c.key)
        self.assertEqual(c.key, c.key)

        key_path = testdata.create_file("morp.key", testdata.get_ascii(100))
        c = Connection(options=dict(keyfile=key_path))
        self.assertNotEqual("", c.key)
        self.assertEqual(c.key, c.key)

        c = Connection(options=dict(key=key, keyfile=key_path))
        self.assertNotEqual("", c.key)
        c2 = Connection(options=dict(key=key, keyfile=key_path))
        self.assertEqual(c.key, c2.key)

    def test_dsn_connection(self):
        tests = [
            (
                'path.to.Interface://127.0.0.1:4151',
                dict(
                    hosts=[('127.0.0.1', 4151)],
                    interface_name="path.to.Interface",
                    name=''
                )
            ),
            (
                'module.path.to.Interface://example.com:4161#name',
                dict(
                    hosts=[('example.com', 4161)],
                    interface_name='module.path.to.Interface',
                    name="name"
                )
            ),
            (
                'module.path.to.Interface://example.com:4161?foo=bar&bar=che#name',
                dict(
                    hosts=[('example.com', 4161)],
                    interface_name='module.path.to.Interface',
                    options={"foo": "bar", "bar": "che"},
                    name="name"
                )
            ),
            (
                "morp.interface.sqs.SQS://AWS_ID:AWS_KEY@?read_lock=120",
                dict(
                    username='AWS_ID',
                    password='AWS_KEY',
                    interface_name='morp.interface.sqs.SQS',
                    options={'read_lock': '120'}
                )
            ),
            (
                "morp.interface.sqs.SQS://AWS_ID:AWS_KEY@",
                dict(
                    username='AWS_ID',
                    password='AWS_KEY',
                    interface_name='morp.interface.sqs.SQS',
                    options={}
                )
            )
        ]

        for t in tests:
            c = DsnConnection(t[0])
            for k, v in t[1].items():
                self.assertEqual(v, getattr(c, k))


