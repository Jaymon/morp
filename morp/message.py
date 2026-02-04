# -*- coding: utf-8 -*-
import os
from contextlib import contextmanager
from contextlib import asynccontextmanager, AbstractAsyncContextManager
import logging
import datetime
import inspect
import typing

from datatypes import (
    ReflectClass,
    ReflectName,
    ReflectType,
    make_dict,
    classproperty,
)

from .compat import *
from .interface import get_interface
from .exception import ReleaseMessage, AckMessage
from .config import environ


logger = logging.getLogger(__name__)


class Message(object):
    """
    this is the base class for sending and handling a message

    to add a new message to your application, just subclass this class

    :example:
        import morp

        class CustomMessage(morp.Message):
            def handle(self):
                # target will be called when the message is consumed using
                # the CustomMessage.handle method
                pass

        m1 = CustomMessage(foo=1, bar="one")
        await m1.send() # m1 is sent with foo and bar fields

        m2 = await CustomMessage.create(foo=2, bar="two")
        # m2 was created and sent with foo and bar fields

        await CustomMessage.process(2)
        # both m1 and m2 were consumed and their .target methods called

    By default, all subclasses will go to the same queue and then when the
    queue is consumed the correct child class will be created and consume the
    message with its `.handle` method.

    If you would like your subclass to use a different queue then just set the
    `._name` property on the class and it will use a different queue
    """
    _connection_name: str = ""
    """the name of the connection to use to retrieve the interface"""

#     fields = None
    """holds the actual message that will be sent, this is a dict of key/values
    that will be sent. The fields can be set using properties of this class

    example --
        m = Message
        m.foo = 1
        m.bar = 2
        print(m.fields) # {"foo": 1, "bar": 2}
    """

    _name: str = "morp-messages"
    """The queue name, see .get_name()"""

    _classpath_key: str = "_classpath"
    """The key that will be used to hold the Message's child class's full
    classpath, see `._to_interface` and `.from_interface`"""

    #_id: str = ""
    """The unique id of the message. This is only populated after the
    message has been sent"""

    #_classpath: str
    """holds the Message child class's full classpath, see `._hydrate`"""

    #_count: int = 0
    """How many times the message has been processed"""

    #_raw_send: typing.Any|None = None
    """Holds the raw response from the interface from the last time the
    message was sent"""

    #_raw_recv: typing.Any|None = None
    """Holds the raw response from the interface from the last time the
    message was received"""

    @classproperty
    def interface(cls):
        return get_interface(cls._connection_name)

    @classproperty
    def schema(cls):
        schema = {}
        magic_field_names = set(["_id", "_count"])

        for field_name, field_type in typing.get_type_hints(cls).items():
            if (
                field_name.startswith("_")
                and field_name not in magic_field_names
            ):
                continue

            schema[field_name] = ReflectType(field_type)

        # cache the value so we don't need to generate it again
        cls.schema = schema
        return cls.schema

    @property
    def fields(self):
        fields = {}

        for field_name in self.schema.keys():
            v = getattr(self, field_name, typing.NoReturn)
            if v is not typing.NoReturn:
                fields[field_name] = v

        return fields



        #for klass in inspect.getmro(cls)[:-1]:
        #    pout.v(klass.__name__)
#         for k, v in inspect.getmembers_static(cls):
#             # ignore private members
#             if k.startswith("_"):
#                 continue
# 
#             # ignore methods
#             #if inspect.isfunction(v):
#             #    continue
# 
# #             if inspect.isfunction(v) or inspect.iscoroutinefunction(v):
# #                 continue
# 
# #                 if (
# #                     inspect.ismethoddescriptor(v)
# #                     or inspect.isdatadescriptor(v)
# #                     or inspect.isgetsetdescriptor(v)
# #                 ):
# #                     continue
# 
#             pout.v(k, v)


        # we get all the members this way because if we used
        # `inspect.getmembers` trying to get this class property will throw
        # it into an infinite loop
#         for klass in inspect.getmro(cls)[:-1]:
#             for k, v in vars(klass).items():
#                 pout.v(k)



    def __init__(self, fields=None, **fields_kwargs):
        fields = make_dict(fields, fields_kwargs)
        self._from_interface(fields)

        #self.fields = self.make_dict(fields, fields_kwargs)

#     def __getattr__(self, key):
#         if hasattr(self.__class__, key):
#             return super().__getattr__(key)
#         else:
#             return self.fields[key]
# 
#     def __setattr__(self, key, val):
#         if hasattr(self.__class__, key):
#             super().__setattr__(key, val)
#         else:
#             self.fields[key] = val
# 
#     def __setitem__(self, key, val):
#         self.fields[key] = val
# 
#     def __getitem__(self, key):
#         return self.fields[key]

    def __contains__(self, field_name):
        v = getattr(self, field_name, typing.NoReturn)
        return v is not typing.NoReturn
        #return hasattr(self, key)
        #return key in self.fields

    async def send(self, **kwargs):
        """send the message using the configured interface for this class

        :param **kwargs:
        :keyword delay_seconds: int, how many seconds before the message can
            be processed. The max value is interface specific, (eg, it can
            only be 900s max (15 minutes) per SQS docs)
        """
        queue_off = environ.DISABLED
        name = self.get_name()
        fields = self._to_interface()
        if queue_off:
            logger.warning("DISABLED - Would have sent {} to {}".format(
                fields,
                name,
            ))

        else:
            logger.info("Sending message with '{}' keys to '{}'".format(
                "', '".join(fields.keys()),
                name
            ))

            hydrate_fields = await self.interface.send(
                name=name,
                fields=fields,
                **kwargs,
            )
            # we mimic hydration
            self._from_interface(hydrate_fields)
            self._hydrate_fields = hydrate_fields

    @classmethod
    def get_name(cls):
        """This is what's used as the official queue name, it takes `.name`
        and combines it with the `MORP_PREFIX` environment variable

        :returns: str, the queue name
        """
        name = cls._name
        if env_name := environ.PREFIX:
            name = "{}-{}".format(env_name, name)
        return name

    @classmethod
    async def process(cls, count=0, **kwargs):
        """wait for messages to come in and handle them by calling the incoming
        message's `handle` method

        :example:
            # handle 10 messages by receiving them and calling `handle` on the
            # message instance
            Message.process(10)

        :param count: int, if you only want to handle N messages, pass in count
        :param **kwargs: any other params will get passed to underlying 
            `._recv` methods
        """
        max_count = count
        count = 0
        while not max_count or count < max_count:
            count += 1
            logger.debug("Receiving {}/{}".format(
                count,
                max_count if max_count else "Infinity"
            ))

            async with cls._recv(**kwargs) as m:
                r = m.handle()
                while inspect.iscoroutine(r):
                    r = await r

                if r is False:
                    raise ReleaseMessage()

    @classmethod
    @asynccontextmanager
    async def _recv(cls, **kwargs) -> AbstractAsyncContextManager:
        """Internal context manager that receives a message to be processed

        Called from `.process` to receive messages and repeatedly calls
        `._recv_for` to actually receive a message

        :keyword timeout: int, how long to wait before yielding None
        """
        m = None
        # 20 is the max long polling timeout per Amazon
        kwargs.setdefault('timeout', 20)
        while not m:
            async with cls._recv_for(**kwargs) as m:
                if m:
                    yield m

    @classmethod
    @asynccontextmanager
    async def _recv_for(
        cls,
        timeout: int|float,
        **kwargs,
    ) -> AbstractAsyncContextManager:
        """Internal context manager. Try and receive a message, return None
        if a message is not received within timeout

        :param timeout: float|int, how many seconds before yielding None
        :returns: generator[Message]
        """
        i = cls.interface
        name = cls.get_name()
        ack_on_recv = kwargs.pop('ack_on_recv', False)
        logger.debug(
            "Waiting to receive on {} for {} seconds".format(name, timeout)
        )

        async with i.connection(**kwargs) as connection:
            kwargs["connection"] = connection

            fields = await i.recv(name, timeout=timeout, **kwargs)
            if fields:
                try:
                    yield cls._hydrate(fields)

                except ReleaseMessage as e:
                    await i.release(
                        name,
                        fields,
                        delay_seconds=e.delay_seconds,
                        **kwargs
                    )

                except AckMessage as e:
                    await i.ack(name, fields, **kwargs)

                except Exception as e:
                    if ack_on_recv:
                        await i.ack(name, fields, **kwargs)

                    else:
                        await i.release(name, fields, **kwargs)

                    raise

                else:
                    await i.ack(name, fields, **kwargs)

            else:
                yield None

    @classmethod
    async def create(cls, *args, **kwargs):
        """create an instance of cls with the passed in fields and send it off

        Since this passed *args and **kwargs directly to .__init__, you can
        override the .__init__ method and customize it and this method will
        inherit the child class's changes. And the signature of this method
        should always match .__init__

        :param *args: list[Any], passed directly to .__init__
        :param **kwargs: dict[str, Any], passed directly to .__init__
        """
        connection = kwargs.pop("connection", None)
        instance = cls(*args, **kwargs)
        await instance.send(connection=connection)
        return instance

#     @classmethod
#     async def unsafe_clear(cls):
#         """clear the whole message queue"""
#         n = cls.get_name()
#         return await cls.interface.unsafe_clear(n)

    @classmethod
    async def count(cls):
        """how many messages total (approximately) are in the whole message
        queue"""
        n = cls.get_name()
        return await cls.interface.count(n)

#     @classmethod
#     def _make_dict(cls, fields, fields_kwargs):
#         """lot's of methods take a dict or kwargs, this combines those"""
#         return make_dict(fields, fields_kwargs)

#     @classmethod
#     def get_class(cls, classpath):
#         """wrapper to make it easier to do this in child classes, which seems
#         to happen quite frequently"""
#         return ReflectName(classpath).get_class()

    @classmethod
    def _hydrate(cls, fields):
        """This is used by the interface to populate an instance with
        information received from the interface

        :param fields: InterfaceMessage, the message freshly received from
            the interface, see Interface.create_imessage()
        """
        message_class = cls
        if cls is Message:
            # When a generic Message instance is used to consume messages it
            # will use the passed in classpath to create the correct Message
            # child
            rn = ReflectName(fields.get(cls._classpath_key))
            message_class = rn.get_class()

        instance = message_class()
        instance._from_interface(fields)
        instance._hydrate_fields = fields

        return instance

    def _to_interface(self):
        """When sending a message to the interface this method will be called

        :returns: dict, the fields
        """
        fields = {}
        schema = self.schema
        for field_name, rt in schema.items():
            fields[field_name] = rt.cast(getattr(self, field_name))

        if self._classpath_key not in fields:
            fields[self._classpath_key] = ReflectClass(self).classpath

        return fields

    def _from_interface(self, fields):
        """When receiving a message from the interface this method will
        be called

        you can see it in action with `._hydrate`

        :param fields: dict, the fields received from the interface
        """
        for field_name in self.schema.keys():
            if field_name in fields:
                setattr(self, field_name, fields[field_name])

#         for field_name, field_value in fields.items():
#             if field_name in schema:
#                 setattr(self, field_name, field_value)

    async def handle(self):
        """This method will be called from handle() and can handle any
        processing of the message, it should be defined in the child classes"""
        raise NotImplementedError()

#     async def ack(self, **kwargs):
#         """Acknowledge this message has been processed"""
#         await self.interface.ack(
#             self.get_name(),
#             self._to_interface(),
#             **kwargs,
#         )
# 
#     async def release(self, **kwargs):
#         """Release this message back to the interface so another message
#         instance can pick it up
# 
#         :param **kwargs:
#             - delay_seconds: int, how many seconds before the message can be
#                 processed again. The max value is interface specific
#         """
#         await self.interface.release(
#             self.get_name(),
#             self._to_interface(),
#             **kwargs,
#         )

