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
        class FooMsg(Message):
            def handle(self):
                # all sent messages will end up in this method to be processed
                print self.bar
                pass

        f = FooMsg()
        f.bar = "some value"
        f.send()

        # in some other script consume our Foo messages
        morp.consume('Foo') # this will call FooMsg.handle() for every FooMsg received
    """

    connection_name = ""
    """the name of the connection to use to retrieve the interface"""

    name = ""
    """the message name (this is usually the topic or whatnot where the message should be sent)"""

    msg = None
    """holds the actual message that will be sent"""

    interface_msg = None
    """this holds the raw message that is returned from a message handler, it is None on send, set on receive"""

    @decorators.classproperty
    def interface(cls):
        return get_interface(cls.connection_name)

    @decorators.classproperty
    def class_name(cls):
        return ".".join([cls.__module__, cls.__name__])

    def __init__(self, name="", **msg_kwargs):
        if name:
            self.name = name
        else:
            self.name = self.__class__.__name__

        self.msg = msg_kwargs

    def __getattr__(self, key):
        if hasattr(self.__class__, key):
            return super(Message, self).__getattr__(key)
        else:
            return self.msg[key]

    def __setattr__(self, key, val):
        if hasattr(self.__class__, key):
            super(Message, self).__setattr__(key, val)
        else:
            self.msg[key] = val

    def __setitem__(self, key, val):
        self.msg[key] = val

    def __getitem__(self, key):
        return self.msg[key]

    def __contains__(self, key):
        return key in self.msg

    def send(self):
        """send the message using the configured interface for this class"""
        i = self.interface
        return i.send(self)

    def handle(self):
        """all consumed messages will have this method called when the message is received"""
        raise NotImplementedError("To Process {} messages override this method".format(self.name))

    def __str__(self):
        return "{} - {}".format(self.name, self.msg)

