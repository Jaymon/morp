# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import os
from contextlib import contextmanager
import logging
import datetime

from .compat import *
from . import decorators
from .reflection import get_class
from .interface import get_interface
from .exception import ReleaseMessage, AckMessage


logger = logging.getLogger(__name__)


class Message(object):
    """
    this is the base class for sending and handling a message

    to add a new message to your application, just subclass this class
    """

    """
    Message to be consumed by the generic consumer and passed off the the appropriate
    consumer.
    To use, this class should be extended with a function named 'process' that
    accepts a Morp message object.
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

    interface_message = None
    """this will hold the interface message that was used to send this instance
    to the backend using interface"""

    name = "morp-messages"
    """The queue name, see get_name()"""

    @decorators.classproperty
    def interface(cls):
        return get_interface(cls.connection_name)

    def __new__(cls, fields=None, **fields_kwargs):
        if cls is Message:
            # When a generic Message object is created directly it will use the
            # passed in morp_classpath to create the correct Message child
            fields = cls.make_dict(fields, fields_kwargs)
            klass = get_class(fields['morp_classpath'])
            return super(Message, klass).__new__(klass)

        else:
            # When a subclass object is created
            return super(Message, cls).__new__(cls)

    def __init__(self, fields=None, **fields_kwargs):
        fields = self.make_dict(fields, fields_kwargs)
        self.interface_message = fields.pop("interface_message", None)
        fields.setdefault("morp_classpath", ".".join([
            self.__module__,
            self.__class__.__name__
        ]))
        self.fields = fields

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
        if queue_off:
            logger.warn("DISABLED - Would have sent {} to {}".format(
                self.fields,
                self.get_name()
            ))

        else:
            name = self.get_name()
            fields = self.fields
            i = self.interface
            interface_msg = self.interface.create_message(name=name, fields=fields)
            self.interface_message = interface_msg
            logger.info("Sending message with {} keys to {}".format(fields.keys(), name))
            i.send(name, interface_msg, **kwargs)

    def send_later(self, delay_seconds, **kwargs):
        """Send the message after delay_seconds

        :param delay_seconds: int, up to 900 (15 minutes) per SQS docs
        """
        kwargs["delay_seconds"] = delay_seconds
        return self.send(**kwargs)

    @classmethod
    def get_name(cls):
        name = cls.name if cls.name else cls.__name__
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
                m = cls(interface_msg.fields)
                m.interface_message = interface_msg
                yield m

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
    def clear(cls):
        """clear the whole message queue"""
        n = cls.get_name()
        return cls.interface.clear(n)

    @classmethod
    def count(cls):
        """how many messages total (approximately) are in the whole message queue"""
        n = cls.get_name()
        return cls.interface.count(n)

    @classmethod
    def make_dict(cls, fields, fields_kwargs):
        """lot's of methods take a dict or kwargs, this combines those"""
        if not fields: fields = {}
        if fields_kwargs:
            fields.update(fields_kwargs)

        return fields

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

