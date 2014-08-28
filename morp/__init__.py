import os
from contextlib import contextmanager

from .config import DsnConnection, Connection
from . import decorators
from .interface import get_interface, set_interface, get_interfaces, get_class

__version__ = '0.1'

def configure_environ(dsn_env_name='MORP_DSN'):
    """
    configure interfaces based on environment variables

    by default, when morp is imported, it will look for MORP_DSN, and MORP_DSN_N (where
    N is 1 through infinity) in the environment, if it finds them, it will assume they
    are dsn urls that morp understands and will configure connections with them. If you
    don't want this behavior (ie, you want to configure morp manually) then just make sure
    you don't have any environment variables with matching names

    The num checks (eg MORP_DSN_1, MORP_DSN_2) go in order, so you can't do MORP_DSN_1, MORP_DSN_3,
    because it will fail on _2 and move on, so make sure your num dsns are in order (eg, 1, 2, 3, ...)

    dsn_env_name -- string -- the name of the environment variables prefix
    """
    if dsn_env_name in os.environ:
        configure(os.environ[dsn_env_name])

    # now try importing 1 -> N dsns
    increment_name = lambda name, num: '{}_{}'.format(name, num)
    dsn_num = 1
    dsn_env_num_name = increment_name(dsn_env_name, dsn_num)
    if dsn_env_num_name in os.environ:
        try:
            while True:
                configure(os.environ[dsn_env_num_name])
                dsn_num += 1
                dsn_env_num_name = increment_name(dsn_env_name, dsn_num)

        except KeyError:
            pass


def configure(dsn):
    """
    configure an interface to be used to send messages to a backend

    you use this function to configure an Interface using a dsn, then you can get
    that interface using the get_interface() method

    dsn -- string -- a properly formatted prom dsn, see DsnConnection for how to format the dsn
    """
    #global interfaces

    c = DsnConnection(dsn)
    if c.name in get_interfaces():
        raise ValueError('a connection named "{}" has already been configured'.format(c.name))

    interface_class = get_class(c.interface_name)
    i = interface_class(c)
    set_interface(i, c.name)
    return i


configure_environ()


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
    @contextmanager
    def recv(cls, **kwargs):
        i = cls.interface
        name = cls.get_name()
        interface_msg = i.recv(name, **kwargs)
        yield cls.create(interface_msg.msg)
        i.ack(name, interface_msg)

    @classmethod
    def get_name(cls):
        return cls.__name__

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

