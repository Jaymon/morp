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


class Client(object):
    def __init__(self, contents):
        module_info = testdata.create_module(contents=contents)
        self.directory = module_info.basedir
        self.module = module_info.module
        self.message_classes = []

        clear_names = {}
        for _, message_class in inspect.getmembers(self.module, inspect.isclass):
            if issubclass(message_class, Message):
                clear_names[message_class.get_name()] = message_class
                self.message_classes.append(message_class)

        for message_class in clear_names.values():
            message_class.clear()

    def send(self, **fields):
        return self.message_classes[0].create(fields)

    def recv(self):
        return self.run(self.message_classes[0].name)

    def run(self, name, count=1, **options):
        python_cmd = String(subprocess.check_output(["which", "python"]).strip())
        cmd = "{} -m morp --count={} --directory={} {}".format(
            python_cmd,
            count,
            self.directory,
            name
        )
        expected_ret_code = options.get('code', 0)

        is_py2 = True
        is_py3 = False

        def get_output_str(output):
            return "\n".join(String(o) for o in output)
#             if is_py2:
#                 return "\n".join(output)
#             elif is_py3:
#                 return "\n".join((o.decode("utf-8") for o in output))

        process = None
        output = []
        try:
            process = subprocess.Popen(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=os.getcwd(),
            )

            for line in iter(process.stdout.readline, b""):
                line = line.rstrip()
                print(line)
                output.append(line)

            process.wait()
            if process.returncode != expected_ret_code:
                raise RuntimeError("cmd returned {} with output: {}".format(
                    process.returncode,
                    get_output_str(output)
                ))

        except subprocess.CalledProcessError as e:
            raise RuntimeError("cmd returned {} with output: {}".format(e.returncode, e.output))

        finally:
            if process:
                process.stdout.close()

        return get_output_str(output)


class BaseInterfaceTestCase(TestCase):
    interface_class = None

    interfaces = defaultdict(list)

    def setUp(self):
        i = self.get_interface()
        n = self.get_name()
        i.clear(n)

    @classmethod
    def tearDownClass(cls):
        # clean up all the queues we made and close all the interfaces
        for name, interfaces in cls.interfaces.items():
            for i in interfaces:
                if i:
                    if name:
                        i.unsafe_delete(name)
                    i.close()

    def create_message(self, name="", interface=None, **fields):
        name = self.get_name(name)
        interface = interface or self.get_interface()

        if not fields:
            fields[testdata.get_ascii()] = testdata.get_int()
            fields[testdata.get_ascii()] = testdata.get_int()

        msg = interface.create_message(name, fields=fields)
        type(self).interfaces[name].append(interface)
        return msg

    def get_config(self, dsn="", **options):
        dsn = dsn or os.environ['MORP_DSN_1']
        config = DsnConnection(os.environ['MORP_DSN_1'])
        for k, v in options.items():
            config.options[k] = v
        return config

    def get_interface(self, config=None):
        """get a connected interface"""
        config = config or self.get_config()
        i = self.interface_class(config)
        i.connect()
        type(self).interfaces[""].append(i)
        self.assertTrue(i.connected)
        return i

    def get_encrypted_interface(self, config=None):
        """get a connected interface"""
        options = {}
        if testdata.yes():
            options['key'] = testdata.create_file("/morp.key", testdata.get_ascii(100))
        else:
            options['key'] = testdata.get_ascii(32)

        if config:
            for k, v in options.items():
                config.options[k] = v
        else:
            config = self.get_config(**options)

        return self.get_interface(config)

    def get_name(self, name=""):
        if not name:
            name = 'morp-test-' + testdata.get_ascii(12)
            #name = 'morp-test-sqs'
        type(self).interfaces[name].append(None)
        return name

    def test_message_lifecycle(self):
        i = self.get_encrypted_interface()
        im = i.create_message(name="message-lifecycle")

        fields = {"foo": 1, "bar": 2}
        im.fields = fields
        body = im.body

        im2 = i.create_message(name="message-lifecycle")
        im2.body = body
        self.assertEqual(im.fields, im2.fields)
        self.assertEqual(im.fields, fields)

    def assertEventuallyEqual(self, v1, callback, msg="", count=10, wait=1.0):
        ret = False
        for x in range(count - 1):
            if callback() == v1:
                ret = True
                break

            else:
                time.sleep(wait)

        if not ret:
            self.assertEqual(v1, callback(), msg)


class SQSInterfaceTest(BaseInterfaceTestCase):
    interface_class = SQS

    def test_queue_auto_create(self):
        """SQS queues will auto-create, this just makes sure that works as intended"""
        m = self.create_message()
        name = m.name
        i = m.interface

        i.unsafe_delete(name)

    def test_send_count_recv(self):
        interface_msg = self.create_message()
        interface_msg.send()

        # re-connect to receive the message
        i2 = self.get_interface()
        interface_msg2 = i2.recv(interface_msg.name)
        self.assertEqual(interface_msg.fields, interface_msg2.fields)

        interface_msg2.ack()
        self.assertEventuallyEqual(0, lambda: i2.count(interface_msg.name))

    def test_recv_timeout(self):
        m = self.create_message()
        start = time.time()
        m.interface.recv(m.name, 1)
        stop = time.time()
        self.assertLessEqual(1.0, stop - start)

    def test_send_recv_encrypted(self):
        m1 = self.create_message(interface=self.get_encrypted_interface())
        m1.send()

        m2 = m1.interface.recv(m1.name)
        self.assertEqual(m1.fields, m2.fields)
        m2.ack()

    def test_send_recv_aws_encryption(self):
        config = self.get_config(KmsMasterKeyId="alias/aws/sqs")
        i = self.get_interface(config)

        m1 = self.create_message(interface=i)
        m1.send()

        m2 = m1.interface.recv(m1.name)
        self.assertEqual(m1.fields, m2.fields)
        m2.ack()

    def test_get_attrs(self):
        i = self.get_interface()
        attrs = i.get_attrs(KmsMasterKeyId="foo-bar", KmsDataKeyReusePeriodSeconds=3600)
        self.assertTrue("KmsMasterKeyId" in attrs)


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


class ConnectionTest(TestCase):
    def test_key(self):
        c = Connection()
        self.assertEqual("", c.key)
        self.assertEqual(c.key, c.key)

        key = testdata.get_ascii(100)
        c = Connection(options=dict(key=key))
        self.assertNotEqual(b"", ByteString(c.key))
        self.assertEqual(c.key, c.key)

        key_path = testdata.create_file("morp.key", testdata.get_ascii(100))
        c = Connection(options=dict(key=key_path))
        self.assertNotEqual(b"", ByteString(c.key))
        self.assertEqual(c.key, c.key)

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

    def test_attrs_and_sqs_alias(self):
        c = DsnConnection("SQS://AWS_ID:AWS_KEY@?KmsMasterKeyId=foo-bar")
        self.assertTrue(c.interface_name.startswith("morp"))
        self.assertTrue("KmsMasterKeyId" in c.options)


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

