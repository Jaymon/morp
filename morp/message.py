import os
from contextlib import contextmanager
import logging
import datetime

from . import decorators
from .interface import get_interface


logger = logging.getLogger(__name__)


class Message(object):
    """
    this is the base class for sending and handling a message

    to add a new message to your application, just subclass this class
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

    interface_msg = None
    """this will hold the interface message that was used to send this instance
    to the backend using interface"""

    @decorators.classproperty
    def interface(cls):
        return get_interface(cls.connection_name)

    def __init__(self, fields=None, **fields_kwargs):
        fields = self._normalize_dict(fields, fields_kwargs)
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
        queue_off = bool(int(os.environ.get('MORP_QUEUE_OFF', 0)))
        if queue_off:
            logger.warn("QUEUE OFF - Would have sent {} to {}".format(
                self.fields,
                self.get_name()
            ))

        else:
            name = self.get_name()
            fields = self.fields
            i = self.interface
            interface_msg = self.interface.create_msg(fields=fields)
            self.interface_msg = interface_msg
            logger.info("Sending message with {} keys to {}".format(fields.keys(), name))
            i.send(name, interface_msg, **kwargs)

    @classmethod
    def get_name(cls):
        name = cls.__name__
        env_name = os.environ.get('MORP_QUEUE_PREFIX', '')
        if env_name:
            name = "{}-{}".format(env_name, name)

        return name

    @classmethod
    @contextmanager
    def recv(cls, timeout=None, **kwargs):
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
                m.interface_msg = interface_msg
                yield m

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
    @contextmanager
    def recv_block(cls, **kwargs):
        """similar to recv() but will block until a message is received"""
        m = None
        kwargs.setdefault('timeout', 20) # 20 is the max long polling timeout per Amazon
        while not m:
            with cls.recv(**kwargs) as m:
                if m:
                    yield m

    @classmethod
    def recv_one(cls, timeout=None, **kwargs):
        """this is just syntactic sugar around recv that receives, acknowledges, and
        then returns the message"""
        with cls.recv(timeout=timeout, **kwargs) as m:
            return m

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
        n = cls.get_name()
        return cls.interface.clear(n)

    @classmethod
    def count(cls):
        n = cls.get_name()
        return cls.interface.count(n)

    @classmethod
    def _normalize_dict(cls, fields, fields_kwargs):
        """lot's of methods take a dict or kwargs, this combines those"""
        if not fields: fields = {}
        if fields_kwargs:
            fields.update(fields_kwargs)

        return fields

