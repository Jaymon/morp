# -*- coding: utf-8 -*-

import testdata
from testdata import IsolatedAsyncioTestCase

from morp.extras.message import AsyncMessage

from . import TestCase


class TestAsyncMessage(IsolatedAsyncioTestCase, TestCase):
    def get_message_class(self, **kwargs):
        kwargs.setdefault("message_class", AsyncMessage)
        return super().get_message_class(**kwargs)

    async def test_async(self):
        d = {}

        async def target(mself):
            d["target"] = True

        message_class = self.get_message_class(target=target)

        m = await message_class.create(self.get_fields())
        await message_class.handle(1)

        self.assertTrue(d["target"])

