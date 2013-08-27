import importlib
import logging
import json
import types

logger = logging.getLogger(__name__)

def get_class(full_python_class_path):
    """
    take something like some.full.module.Path and return the actual Path class object

    Note -- this will fail when the object isn't accessible from the module, that means
    you can't define your class object in a function and expect this function to work

    example -- this is bad

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

    def connect(self, connection_config=None, *args, **kwargs):
        """
        connect to the interface

        this will set the raw db connection to self.connection

        *args -- anything you want that will help the db connect
        **kwargs -- anything you want that the backend db connection will need to actually connect
        """

        if self.connected: return self.connected

        if connection_config: self.connection_config = connection_config

        self.connected = False
        self._connect(self.connection_config)
        if self.connection:
            self.connected = True
        else:
            raise ValueError("the ._connect() method did not set .connection attribute")

        self.log("Connected")
        return self.connected

    def _connect(self, connection_config):
        """this *MUST* set the self.connection attribute"""
        raise NotImplementedError("this needs to be implemented in a child class")


    def close(self):
        """
        close an open connection
        """
        if not self.connected: return True

        self._close()

        self.connection = None
        self.connected = False
        self.log("Closed Connection")
        return True

    def _close(self):
        pass

    def assure(self):
        """handle any things that need to be done before a query can be performed"""
        self.connect()

    def send(self, message):
        self.assure()

        msg_str = self.normalize_message(message)
        self._send(message.name, msg_str)
        self.log("Message sent to {} -- {}", message.name, msg_str)

    def _send(self, name, msg_str):
        raise NotImplementedError("this needs to be implemented in a child class")

    def normalize_message(self, message):
        d = {}
        d['name'] = message.name
        d['class_name'] = message.class_name
        d['msg'] = message.msg
        return json.dumps(d)

    def denormalize_message(self, msg_str):
        d = json.loads(msg_str)
        c = get_class(d['class_name'])
        m = c()
        m.name = d['name']
        m.msg = d['msg']
        return m

    def consume(self, message_names=None):
        if message_names is None:
            message_names = []
        else:
            if isinstance(message_names, types.StringTypes):
                message_names = [message_names]

        self._consume(message_names)

    def _consume(self, names):
        raise NotImplementedError("this needs to be implemented in a child class")

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
            if isinstance(format_str, Exception):
                logger.exception(format_str, *format_args)
            else:
                if format_args:
                    logger.log(log_level, format_str.format(*format_args))
                else:
                    logger.log(log_level, format_str)

