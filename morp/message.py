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
        return i.send(self.get_name(), self.fields, **kwargs)

    @classmethod
    def get_name(cls):
        return cls.__name__

    @classmethod
    @contextmanager
    def recv(cls, **kwargs):
        i = cls.interface
        name = cls.get_name()
        interface_msg = i.recv(name, **kwargs)
        yield cls.create(interface_msg.fields)
        i.ack(name, interface_msg)

    @classmethod
    def create(cls, fields=None, **fields_kwargs):
        """
        create an instance of cls with the passed in fields and set it into the db
        fields -- dict -- field_name keys, with their respective values
        **fields_kwargs -- dict -- if you would rather pass in fields as name=val, that works also
        """
        instance = cls(fields, **fields_kwargs)
        return instance

    @classmethod
    def _normalize_dict(cls, fields, fields_kwargs):
        """lot's of methods take a dict or kwargs, this combines those"""
        if not fields: fields = {}
        if fields_kwargs:
            fields.update(fields_kwargs)

        return fields

