# -*- coding: utf-8 -*-
import json

from morp.compat import *
#from morp.interface.base import InterfaceMessage
from . import TestCase, testdata


class InterfaceMessageTest(TestCase):
    def test_serializer_json(self):
        im = self.get_imessage(
            interface=self.get_interface(serializer="json")
        )

        body = im.to_interface()
        self.assertEqual(ByteString(json.dumps(im.fields)), body)

        im2 = self.get_imessage(
            interface=im.interface
        )
        im2.from_interface(body)
        self.assertEqual(im.fields, im2.fields)

    def test_serializer_2(self):
        interfaces = [
            self.get_interface(serializer="pickle"),
            self.get_interface(serializer="json"),
            self.get_encrypted_interface(serializer="pickle"),
            self.get_encrypted_interface(serializer="json"),
        ]

        for interface in interfaces:
            im = self.get_imessage(
                interface=interface,
            )

            fields = im.fields
            body = im.to_interface()
            self.assertTrue(isinstance(body, bytes))

            im.from_interface(body)
            self.assertEqual(fields, im.fields)
            self.assertEqual(body, im.body)

