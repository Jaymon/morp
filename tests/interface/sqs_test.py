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
#from morp.compat import *
#from morp import Message, Connection, DsnConnection
from morp.interface.sqs import SQS
#from morp.interface import get_interfaces
#from morp.exception import ReleaseMessage, AckMessage


class SQSInterfaceTest(BaseTestInterface):
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


