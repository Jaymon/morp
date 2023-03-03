# -*- coding: utf-8 -*-

from morp.compat import *
from morp.interface.sqs import SQS
from . import _InterfaceTest

class InterfaceTest(_InterfaceTest):
    interface_class = SQS

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


