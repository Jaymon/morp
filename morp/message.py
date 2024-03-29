# -*- coding: utf-8 -*-
import os
from contextlib import contextmanager
import logging
import datetime

from datatypes import ReflectClass, make_dict, classproperty

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
            def target(self):
                # target will be called when the message is consumed using
                # the CustomMessage.handle method
                pass

        m1 = CustomMessage(foo=1, bar="one")
        m1.send() # m1 is sent with foo and bar fields

        m2 = CustomMessage.create(foo=2, bar="two")
        # m2 was created and sent with foo and bar fields

        CustomMessage.handle(2)
        # both m1 and m2 were consumed and their .target methods called

    By default, all subclasses will go to the same queue and then when the queue
    is consumed the correct child class will be created and consume the message
    with its .target() method.

    If you would like your subclass to use a different queue then just set .name
    property on the class and it qill use a different queue
    """
    connection_name = ""
    """the name of the connection to use to retrieve the interface"""

    fields = None
    """holds the actual message that will be sent, this is a dict of key/values
    that will be sent. The fields can be set using properties of this class

    example --
        m = Message
        m.foo = 1
        m.bar = 2
        print(m.fields) # {"foo": 1, "bar": 2}
    """

    name = "morp-messages"
    """The queue name, see .get_name()"""

    classpath_key = "_message_classpath"
    """The key that will be used to hold the Message's child class's full
    classpath, see .hydrate()"""

    @classproperty
    def interface(cls):
        return get_interface(cls.connection_name)

    def __init__(self, fields=None, **fields_kwargs):
        self.fields = self.make_dict(fields, fields_kwargs)

    def __getattr__(self, key):
        if hasattr(self.__class__, key):
            return super().__getattr__(key)
        else:
            return self.fields[key]

    def __setattr__(self, key, val):
        if hasattr(self.__class__, key):
            super().__setattr__(key, val)
        else:
            self.fields[key] = val

    def __setitem__(self, key, val):
        self.fields[key] = val

    def __getitem__(self, key):
        return self.fields[key]

    def __contains__(self, key):
        return key in self.fields

    def send(self, **kwargs):
        """send the message using the configured interface for this class

        :param **kwargs:
            - delay_seconds: int, how many seconds before the message can be
                processed. The max value is interface specific, (eg, it can only
                be 900s max (15 minutes) per SQS docs)
        """
        queue_off = environ.DISABLED
        name = self.get_name()
        fields = self.to_interface()
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
            self.from_interface(
                self.interface.send(name=name, fields=fields, **kwargs)
            )

    @classmethod
    def get_name(cls):
        """This is what's used as the official queue name, it takes cls.name
        and combines it with MORP_PREFIX environment variable

        :returns: str, the queue name
        """
        name = cls.name
        if env_name := environ.PREFIX:
            name = "{}-{}".format(env_name, name)
        return name

    @classmethod
    @contextmanager
    def recv(cls, block=True, **kwargs):
        """Try and receive a message, this is usually used as a context manager

        Usually you'll want to use .handle(), since that will automatically call
        the message's .target() method, but if you want to do something custom
        with the message then you can call this method directly

        :Example:
            with Message.recv() as m:
                print(m.fields)

        :param block: bool, if True this will block until it receives a message
        :param **kwargs:
            * timeout: int, how long to wait before yielding None
        :returns: generator<Message>
        """
        if block:
            m = None
            # 20 is the max long polling timeout per Amazon
            kwargs.setdefault('timeout', 20)
            while not m:
                with cls.recv_for(**kwargs) as m:
                    if m:
                        yield m

        else:
            kwargs.setdefault('timeout', 1)
            with cls.recv_for(**kwargs) as m:
                if m:
                    yield m

    @classmethod
    @contextmanager
    def recv_for(cls, timeout, **kwargs):
        """try and receive a message, return None if a message is not received
        within timeout

        Internal method, you'll notice .recv() calls this method. This method
        attempts to get a message and will ack or release the message depending
        on what .target() did

        :param timeout: float|int, how many seconds before yielding None
        :returns: generator[Message]
        """
        i = cls.interface
        name = cls.get_name()
        ack_on_recv = kwargs.pop('ack_on_recv', False)
        logger.debug(
            "Waiting to receive on {} for {} seconds".format(name, timeout)
        )

        with i.connection(name, **kwargs) as connection:
            kwargs["connection"] = connection

            fields = i.recv(name, timeout=timeout, **kwargs)
            if fields:
                try:
                    yield cls.hydrate(fields)

                except ReleaseMessage as e:
                    i.release(
                        name,
                        fields,
                        delay_seconds=e.delay_seconds,
                        **kwargs
                    )

                except AckMessage as e:
                    i.ack(name, fields, **kwargs)

                except Exception as e:
                    if ack_on_recv:
                        i.ack(name, fields, **kwargs)

                    else:
                        i.release(name, fields, **kwargs)

                    raise

                else:
                    i.ack(name, fields, **kwargs)

            else:
                yield None


    @classmethod
    def handle_iter(cls, count):
        max_count = count
        count = 0
        while not max_count or count < max_count:
            count += 1
            logger.debug("Handling {}/{}".format(
                count,
                max_count if max_count else "Infinity"
            ))

            yield count

    @classmethod
    def handle(cls, count=0, **kwargs):
        """wait for messages to come in and handle them by calling the incoming
        message's target() method

        :Example:
            # handle 10 messages by consuming them and calling .target()
            Message.handle(10)

        :param count: int, if you only want to handle N messages, pass in count
        :param **kwargs: any other params will get passed to underlying recv
            methods
        """
        for x in cls.handle_iter(count):
            with cls.recv(**kwargs) as m:
                r = m.target()

                if r is False:
                    raise ReleaseMessage()

    @classmethod
    def create(cls, *args, **kwargs):
        """create an instance of cls with the passed in fields and send it off

        Since this passed *args and **kwargs directly to .__init__, you can
        override the .__init__ method and customize it and this method will
        inherit the child class's changes. And the signature of this method
        should always match .__init__

        :param *args: list[Any], passed directly to .__init__
        :param **kwargs: dict[str, Any], passed directly to .__init__
        """
        instance = cls(*args, **kwargs)
        instance.send()
        return instance

    @classmethod
    def unsafe_clear(cls):
        """clear the whole message queue"""
        n = cls.get_name()
        return cls.interface.unsafe_clear(n)

    @classmethod
    def count(cls):
        """how many messages total (approximately) are in the whole message
        queue"""
        n = cls.get_name()
        return cls.interface.count(n)

    @classmethod
    def make_dict(cls, fields, fields_kwargs):
        """lot's of methods take a dict or kwargs, this combines those"""
        return make_dict(fields, fields_kwargs)

    @classmethod
    def get_class(cls, classpath):
        """wrapper to make it easier to do this in child classes, which seems to
        happen quite frequently"""
        return ReflectClass.resolve_class(classpath)

    @classmethod
    def hydrate(cls, fields):
        """This is used by the interface to populate an instance with
        information received from the interface

        :param imessage: InterfaceMessage, the message freshly received from the
            interface, see Interface.create_imessage()
        """
        message_class = cls
        if cls is Message:
            # When a generic Message instance is used to consume messages it
            # will use the passed in classpath to create the correct Message
            # child
            message_class = cls.get_class(fields.pop(cls.classpath_key))

        instance = message_class()
        instance.from_interface(fields)

        return instance

    def to_interface(self):
        """When sending a message to the interface this method will be called

        :returns: dict, the fields
        """
        fields = self.fields
        if self.classpath_key not in fields:
            fields[self.classpath_key] = ReflectClass.get_classpath(self)
        return fields

    def from_interface(self, fields):
        """When receiving a messag from the interface this method will be called

        you can see it in action with .hydrate()

        :param fields: dict, the fields received from the interface
        """
        self.fields.update(fields)

    def target(self):
        """This method will be called from handle() and can handle any
        processing of the message, it should be defined in the child classes"""
        raise NotImplementedError()

    def ack(self, **kwargs):
        """Acknowledge this message has been processed"""
        self.interface.ack(self.get_name(), self.to_interface(), **kwargs)

    def release(self, **kwargs):
        """Release this message back to the interface so another message
        instance can pick it up

        :param **kwargs:
            - delay_seconds: int, how many seconds before the message can be
                processed again. The max value is interface specific
        """
        self.interface.release(self.get_name(), self.to_interface(), **kwargs)

