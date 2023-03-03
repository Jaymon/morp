# -*- coding: utf-8 -*-
import json

from morp.compat import *
#from morp.interface.base import InterfaceMessage
from . import TestCase, testdata


class InterfaceMessageTest(TestCase):
    def test_serializer(self):
        interfaces = [
            self.get_interface(serializer="pickle"),
            self.get_interface(serializer="json"),
            self.get_encrypted_interface(serializer="pickle"),
            self.get_encrypted_interface(serializer="json"),
        ]

        for inter in interfaces:
            fields1 = self.get_fields()
            body = inter.fields_to_body(fields1)
            self.assertTrue(isinstance(body, bytes))

            fields2 = inter.body_to_fields(body)
            self.assertEqualFields(fields1, fields2)

