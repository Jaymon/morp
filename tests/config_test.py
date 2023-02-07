# -*- coding: utf-8 -*-

import testdata
from testdata import TestCase

from morp.config import Connection, DsnConnection


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

