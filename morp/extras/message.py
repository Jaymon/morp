# -*- coding: utf-8 -*-
from contextlib import asynccontextmanager

from ..message import Message
from ..exception import ReleaseMessage, AckMessage


class AsyncMessage(Message):
    """Wrapper around Message's public IO methods, while Morp isn't actually
    async some of my other libraries are and I'll need to use them with Morp so
    this wrapper is the intermediate solution while Morp isn't fully async that
    allows me to stay within the async guardrails

    This should be a drop-in replacement for morp.Message with the only
    difference being having to await the IO methods
    """
    async def send(self, **kwargs):
        return super().send(**kwargs)

    @classmethod
    @asynccontextmanager
    async def recv(cls, *args, **kwargs):
        with super().recv(*args, **kwargs) as m:
            yield m

    @classmethod
    async def handle(cls, count=0, **kwargs):
        """Sadly I had to completely reimplement this method

        see parent's .handle method
        """
        for x in cls.handle_iter(count):
            async with cls.recv(**kwargs) as m:
                r = await m.target()

                if r is False:
                    raise ReleaseMessage()

    @classmethod
    async def create(cls, *args, **kwargs):
        """I also had to completely reimplement this method"""
        instance = cls(*args, **kwargs)
        await instance.send()
        return instance

    @classmethod
    async def unsafe_clear(cls):
        return super().unsafe_clear()

    @classmethod
    async def count(cls):
        return super().count()

    async def target(self):
        return super().target()

    async def ack(self, **kwargs):
        return super().ack(**kwargs)

    async def release(self, **kwargs):
        return super().release(**kwargs)

