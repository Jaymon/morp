import nsq

from .config import DsnConnection, Connection
from . import decorators
from . import interface

__version__ = '0.1'

interfaces = {}
"""holds all configured interfaces"""


def configure(dsn):
    """configure a connection from the passed in dsn"""
    raise RuntimeError("this needs to be refactored")

    global connections
    c = DsnConnection(dsn)
    i_classname = interface.get_class(c.interface_name)
    set_interface(c.name, i_classname(c))


def get_interface(connection_name=""):
    """get the configured interface that corresponds to connection_name"""
    global interfaces
    i = interfaces[connection_name]
    return i


def set_interface(connection_name, interface):
    """bind an .interface.Interface() instance to connection_name"""
    global interfaces
    interfaces[connection_name] = interface


def consume(message_names=None, connection_name=""):
    """begin consume the messages with the given message_names, use connection_name to consume them"""
    i = get_interface(connection_name)
    i.consume(message_names)


class Message(object):
    """
    this is the base class for sending and handling a message

    to add a new message to your application, just subclass this class

        # in some script, create and send our new Foo message
        class Foofields(Message):
            def handle(self):
                # all sent messages will end up in this method to be processed
                print self.bar
                pass

        f = Foofields()
        f.bar = "some value"
        f.send()

        # in some other script consume our Foo messages
        morp.consume('Foo') # this will call Foofields.handle() for every FooMsg received
    """

    connection_name = ""
    """the name of the connection to use to retrieve the interface"""

    name = ""
    """the message name (this is usually the topic or whatnot where the message should be sent)"""

    fields = None
    """holds the actual message that will be sent"""

    interface_fields = None
    """this holds the raw message that is returned from a message handler, it is None on send, set on receive"""
    # TODO -- a message lifetime will go through various states, when it is first
    # created it will be UNSENT, after sending, then it will be SENT, that is the
    # last state that message can be in, when a message is received, then it will be
    # UNACKED, and once completed, it will be ACKED, which means it cannot be sent
    # or received again
    # I think ideal is:
    #    with Foo.recv() as f:
    #        # f.ack() will be called at the end
    STATE_UNSENT = 1
    STATE_SENT = 2
    STATE_UNACKED = 3
    STATE_ACKED = 4

    @decorators.classproperty
    def interface(cls):
        return get_interface(cls.connection_name)

    @decorators.classproperty
    def class_name(cls):
        return ".".join([cls.__module__, cls.__name__])

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

    def send(self):
        """send the message using the configured interface for this class"""
        i = self.interface
        return i.send(self)

    def get_name(self):
        name = self.name
        if not name:
            name = self.__class__.__name__
        return name

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

