# -*- coding: utf-8 -*-
import json

from morp.interface.base import InterfaceMessage

from . import TestCase, testdata


class InterfaceMessageTest(TestCase):
    def test_serializer(self):
        name = self.get_name()
        inter = self.get_interface(serializer="json")

        im = InterfaceMessage(
            name=name,
            interface=inter,
            fields=self.get_fields(),
        )

        body = im.to_interface()
        self.assertEqual(json.dumps(im.fields), body)

        im2 = InterfaceMessage(
            name=name,
            interface=inter,
        )
        im2.from_interface(body)
        self.assertEqual(im.fields, im2.fields)

