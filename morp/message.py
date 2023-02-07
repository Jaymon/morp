# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import os
from contextlib import contextmanager
import logging
import datetime

from datatypes import ReflectClass, make_dict, classproperty

from .compat import *
from .interface import get_interface
from .exception import ReleaseMessage, AckMessage


logger = logging.getLogger(__name__)


class Message(object):
    """
    this is the base class for sending and handling a message

    to add a new message to your application, just subclass this class

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

    imessage = None
    """When a Message subclass instance is created on the receiving end it will
    be passed the raw interface message which will be set into this property. This
    is also set when the message instance is sent (see .send())
    """

    name = "morp-messages"
    """The queue name, see .get_name()"""

    classpath_key = "morp_message_classpath"
    """The key that will be used to hold the Message's child class's full classpath,
    see .hydrate()"""

    @classproperty
    def interface(cls):
        return get_interface(cls.connection_name)

    def __init__(self, fields=None, **fields_kwargs):
        self.fields = self.make_dict(fields, fields_kwargs)

    def __getattr__(self, key):
        if hasattr(self.__class__, key):
            return super(Message, self).__getattr__(key)
        else:
            return self.fields[key]

    def __setattr__(self, key, val):
        if hasattr(self.__class__, key):
            super(Message, self).__setattr__(key, val)
        else:
            self.fields[key] = val

    def __setitem__(self, key, val):
        self.fields[key] = val

    def __getitem__(self, key):
        return self.fields[key]

    def __contains__(self, key):
        return key in self.fields

    def send(self, **kwargs):
        """send the message using the configured interface for this class"""
        queue_off = bool(int(os.environ.get('MORP_DISABLED', 0)))
        name = self.get_name()
        fields = self.to_interface()
        if queue_off:
            logger.warning("DISABLED - Would have sent {} to {}".format(
                fields,
                name,
            ))

        else:
            logger.info("Sending message with {} keys to {}".format(fields.keys(), name))
            self.imessage = self.interface.send(name=name, fields=fields, **kwargs)

    def send_later(self, delay_seconds, **kwargs):
        """Send the message after delay_seconds

        :param delay_seconds: int, up to 900 (15 minutes) per SQS docs
        """
        kwargs["delay_seconds"] = delay_seconds
        return self.send(**kwargs)

    @classmethod
    def get_name(cls):
        name = cls.name
        env_name = os.environ.get('MORP_PREFIX', '')
        if env_name:
            name = "{}-{}".format(env_name, name)

        return name

    @classmethod
    @contextmanager
    def recv(cls, block=True, **kwargs):
        if block:
            m = None
            kwargs.setdefault('timeout', 20) # 20 is the max long polling timeout per Amazon
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
        within timeout"""
        i = cls.interface
        name = cls.get_name()
        ack_on_recv = kwargs.pop('ack_on_recv', False)
        logger.debug("Waiting to receive on {} for {} seconds".format(name, timeout))
        interface_msg = i.recv(name, timeout=timeout, **kwargs)
        if interface_msg:
            try:
                yield cls.hydrate(interface_msg)

            except ReleaseMessage as e:
                i.release(name, interface_msg, delay_seconds=e.delay_seconds)

            except AckMessage as e:
                i.ack(name, interface_msg)

            except Exception as e:
                if ack_on_recv:
                    i.ack(name, interface_msg)
                else:
                    i.release(name, interface_msg)

                raise

            else:
                i.ack(name, interface_msg)

        else:
            yield None

    @classmethod
    def handle(cls, count=0, **kwargs):
        """wait for messages to come in and handle them by calling the incoming
        message's target() method

        :param count: int, if you only want to handle N messages, pass in count
        :param **kwargs: any other params will get passed to underlying recv methods
        """
        max_count = count
        count = 0
        while not max_count or count < max_count:
            with cls.recv(**kwargs) as m:
                m.target()
                count += 1

    @classmethod
    def create(cls, fields=None, **fields_kwargs):
        """
        create an instance of cls with the passed in fields and send it off

        fields -- dict -- field_name keys, with their respective values
        **fields_kwargs -- dict -- if you would rather pass in fields as name=val
        """
        instance = cls(fields, **fields_kwargs)
        instance.send()
        return instance

    @classmethod
    def unsafe_clear(cls):
        """clear the whole message queue"""
        n = cls.get_name()
        return cls.interface.unsafe_clear(n)

    @classmethod
    def count(cls):
        """how many messages total (approximately) are in the whole message queue"""
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
        return ReflectClass.get_class(classpath)

    @classmethod
    def hydrate(cls, imessage):
        fields = imessage.fields
        message_class = cls

        if cls is Message:
            # When a generic Message instance is used to consume messages it
            # will use the passed in classpath to create the correct Message child
            message_class = cls.get_class(fields[cls.classpath_key])

        instance = message_class()
        instance.imessage = imessage
        instance.from_interface(fields)

        return instance

    def to_interface(self):
        fields = self.fields
        fields.setdefault(self.classpath_key, ReflectClass.get_classpath(self))
        return fields

    def from_interface(self, fields):
        for k, v in fields.items():
            self.fields[k] = v

    def target(self):
        """This method will be called from handle() and can handle any processing
        of the message"""
        raise NotImplementedError()

    def ack(self):
        interface_msg = self.interface_message
        # ??? - should this throw an exception if interface_msg is None?
        if interface_msg:
            name = cls.get_name()
            cls.interface.ack(name, interface_msg)

    def release(self, **kwargs):
        interface_msg = self.interface_message
        # ??? - should this throw an exception if interface_msg is None?
        if interface_msg:
            name = cls.get_name()
            cls.interface.release(name, interface_msg, **kwargs)

    def release_later(self, delay_seconds):
        return self.release(delay_seconds=delay_seconds)

