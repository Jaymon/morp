from contextlib import contextmanager

from . import decorators
from .interface import get_interface

class Message(object):
    """
    this is the base class for sending and handling a message

    to add a new message to your application, just subclass this class
    """

    connection_name = ""
    """the name of the connection to use to retrieve the interface"""

    fields = None
    """holds the actual message that will be sent"""

    @decorators.classproperty
    def interface(cls):
        return get_interface(cls.connection_name)

    def __init__(self, fields=None, **fields_kwargs):
        self.fields = self._normalize_dict(fields, fields_kwargs)

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
        i = self.interface
        msg = self.interface.create_msg(fields=self.fields)
        return i.send(self.get_name(), msg, **kwargs)

    @classmethod
    def get_name(cls):
        return cls.__name__

    @classmethod
    @contextmanager
    def recv(cls, timeout=None, **kwargs):
        """try and receive a message, return None if a message is not received
        withing timeout"""
        i = cls.interface
        name = cls.get_name()
        interface_msg = i.recv(name, timeout=timeout, **kwargs)
        if interface_msg:
            yield cls(interface_msg.fields)
            i.ack(name, interface_msg)

        else:
            yield None

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

