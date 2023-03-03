# -*- coding: utf-8 -*-

from morp.compat import *
from morp.interface.sqs import SQS
from . import _InterfaceTest

class InterfaceTest(_InterfaceTest):
    interface_class = SQS

    def test_send_recv_aws_encryption(self):
        config = self.get_config(KmsMasterKeyId="alias/aws/sqs")
        inter = self.get_interface(config)
        name = self.get_name()

        fields1 = inter.send(name, self.get_fields())
        fields2 = inter.recv(name)
        self.assertEqualFields(fields1, fields2)

        inter.ack(name, fields2)

    def test_get_attrs(self):
        inter = self.get_interface()
        attrs = inter.get_attrs(KmsMasterKeyId="foo-bar", KmsDataKeyReusePeriodSeconds=3600)
        self.assertTrue("KmsMasterKeyId" in attrs)


