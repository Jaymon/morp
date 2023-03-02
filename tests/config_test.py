# -*- coding: utf-8 -*-

from morp.config import Connection, DsnConnection
from . import TestCase, testdata


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
        c = Connection(options=dict(key=key_path))
        self.assertNotEqual("", c.key)
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
                'module.path.to.Interface://example.com:4161?foo=bar&bar=che&max_timeout=60&backoff_multiplier=1#name',
                dict(
                    hosts=[('example.com', 4161)],
                    interface_name='module.path.to.Interface',
                    options={
                        "foo": "bar",
                        "bar": "che",
                        "max_timeout": "60",
                        "backoff_multiplier": "1",
                    },
                    name="name"
                )
            ),
            (
                "morp.interface.sqs.SQS://AWS_ID:AWS_KEY@?read_lock=120",
                dict(
                    username='AWS_ID',
                    password='AWS_KEY',
                    interface_name='morp.interface.sqs.SQS',
                    options={
                        'read_lock': '120',
                        "max_timeout": 3600,
                        "backoff_multiplier": 5,
                    }
                )
            ),
            (
                "morp.interface.sqs.SQS://AWS_ID:AWS_KEY@",
                dict(
                    username='AWS_ID',
                    password='AWS_KEY',
                    interface_name='morp.interface.sqs.SQS',
                    #options={}
                )
            )
        ]

        for t in tests:
            c = DsnConnection(t[0])
            for k, v in t[1].items():
                self.assertEqual(v, getattr(c, k), k)

    def test_attrs_and_sqs_alias(self):
        c = DsnConnection("SQS://AWS_ID:AWS_KEY@?KmsMasterKeyId=foo-bar")
        self.assertTrue(c.interface_name.startswith("morp"))
        self.assertTrue("KmsMasterKeyId" in c.options)

    def test_serializer(self):
        config = self.get_config(serializer="json")
        self.assertEqual("json", config.options["serializer"])
        self.assertEqual("json", config.serializer)

        config = self.get_config()
        self.assertEqual("pickle", config.serializer)
    
    def test_backoff(self):
        config = self.get_config(backoff_multiplier=1)
        self.assertEqual(1, config.options["backoff_multiplier"])

