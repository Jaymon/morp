import importlib
import logging
import sys
from contextlib import contextmanager
import base64

try:
    import cPickle as pickle
except ImportError:
    import pickle

from ..exception import InterfaceError


logger = logging.getLogger(__name__)


interfaces = {}
"""holds all configured interfaces"""


def get_interfaces():
    return interfaces


def get_interface(connection_name=""):
    """get the configured interface that corresponds to connection_name"""
    global interfaces
    i = interfaces[connection_name]
    return i


def set_interface(connection_name, interface):
    """bind an .interface.Interface() instance to connection_name"""
    global interfaces
    interfaces[connection_name] = interface


def get_class(full_python_class_path):
    """
    take something like some.full.module.Path and return the actual Path class object

    Note -- this will fail when the object isn't accessible from the module, that means
    you can't define your class object in a function and expect this function to work

    example -- THIS IS BAD --
        def foo():
            class FooCannotBeFound(object): pass
            # this will fail
            get_class("path.to.module.FooCannotBeFound")
    """
    module_name, class_name = full_python_class_path.rsplit('.', 1)
    m = importlib.import_module(module_name)
    c = None
    try:
        c = getattr(m, class_name)
    except AttributeError:
        import inspect
        #cs = inspect.getmembers(m, inspect.isclass)
        cs = inspect.getmembers(m)

    return c


class InterfaceMessage(object):
    """this is a thin wrapper around all received interface messages"""
    def __init__(self, msg, raw_msg):
        """
        msg -- mixed -- the original message you passed to the Interface send method
        raw_msg -- mixed -- this is the raw message the interface returned
        """
        self.msg = msg
        self.raw_msg = raw_msg


class Interface(object):
    """base class for interfaces to messaging"""

    connected = False
    """true if a connection has been established, false otherwise"""

    connection = None
    """hold the actual raw connection to the db"""

    connection_config = None
    """a config.Connection() instance"""

    def __init__(self, connection_config=None):
        self.connection_config = connection_config

    def connect(self, connection_config=None):
        """
        connect to the interface

        this will set the raw db connection to self.connection
        """

        if self.connected: return self.connected
        if connection_config: self.connection_config = connection_config

        try:
            self.connected = False
            self._connect(self.connection_config)
            self.connected = True
            self.log("Connected")

        except Exception as e:
            raise self.raise_error(e)

        return self.connected

    def _connect(self, connection_config):
        """this *MUST* set the self.connection attribute"""
        raise NotImplementedError()

    def free_connection(self, connection): pass

    def get_connection(self): raise NotImplementedError()

    def close(self):
        """
        close an open connection
        """
        if not self.connected: return;

        self._close()
        self.connected = False
        self.log("Closed Connection")

    def _close(self): raise NotImplementedError()

    @contextmanager
    def connection(self, connection=None, **kwargs):
        try:
            if connection:
                yield connection

            else:
                if not self.connected: self.connect()
                try:
                    connection = self.get_connection()
                    yield connection

                except:
                    raise

                finally:
                    self.free_connection(connection)

        except Exception as e:
            self.raise_error(e)

    def send(self, name, msg, **kwargs):
        with self.connection(**kwargs) as connection:
            msg_str = self.normalize_message(msg)
            self._send(name, msg_str, connection=connection)
            self.log("Message sent to {} -- {}", name, msg)

    def _send(self, name, msg_str, connection, **kwargs): raise NotImplementedError()

    def normalize_message(self, msg):
        return base64.b64encode(pickle.dumps(msg, pickle.HIGHEST_PROTOCOL))

    def denormalize_message(self, msg_str):
        return pickle.loads(base64.b64decode(msg_str))

    def count(self, name, **kwargs):
        with self.connection(**kwargs) as connection:
            ret = int(self._count(name, connection=connection))
            return ret

    def _count(self, names, connection, **kwargs): raise NotImplementedError()

    def recv(self, name, **kwargs):
        """receive a message

        return -- tuple -- (msg, interface_msg) -- the msg is the original message
        you passed in, the interface_message is the wrapper most interfaces 
        """
        with self.connection(**kwargs) as connection:
            msg_str, raw_msg = self._recv(name, connection=connection)
            msg = self.denormalize_message(msg_str)
            self.log("Message received from {} -- {}", name, msg)
            return InterfaceMessage(msg, raw_msg)

    def _recv(self, name, connection, **kwargs): raise NotImplementedError()

    def ack(self, name, interface_msg, **kwargs):
        """this will acknowledge that the interface message was received successfully

        an interface_msg is different from a message passed to self.send(), it is
        the InterfaceMessage instance that self.recv() returns
        """
        with self.connection(**kwargs) as connection:
            self._ack(name, interface_msg, connection=connection)
            self.log("Message acked from {} -- {}", name, interface_msg.msg)

    def _ack(self, name, interface_msg, connection, **kwargs): raise NotImplementedError()

    def clear(self, name, **kwargs):
        with self.connection(**kwargs) as connection:
            self._clear(name, connection=connection)
            self.log("Messages cleared from {}", name)

    def _clear(self, name, connection, **kwargs): raise NotImplementedError()

    def log(self, format_str, *format_args, **log_options):
        """
        wrapper around the module's logger

        format_str -- string -- the message to log
        *format_args -- list -- if format_str is a string containing {}, then format_str.format(*format_args) is ran
        **log_options --
        level -- something like logging.DEBUG
        """
        log_level = log_options.get('level', logging.DEBUG)
        if logger.isEnabledFor(log_level):
            try:
                if isinstance(format_str, Exception):
                    logger.exception(format_str, *format_args)
                else:
                    if format_args:
                        logger.log(log_level, format_str.format(*format_args))
                    else:
                        logger.log(log_level, format_str)

            except UnicodeError as e:
                logger.error("Unicode error while logging", exc_info=True)

    def raise_error(self, e, exc_info=None):
        """this is just a wrapper to make the passed in exception an InterfaceError"""
        if not exc_info:
            exc_info = sys.exc_info()
        if not isinstance(e, InterfaceError):
            e = InterfaceError(e, exc_info)
        raise e.__class__, e, exc_info[2]

