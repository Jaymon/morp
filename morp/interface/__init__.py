import logging
import sys
from contextlib import contextmanager
import base64

try:
    import cPickle as pickle
except ImportError:
    import pickle

from Crypto import Random
from Crypto.Cipher import AES

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


def set_interface(interface, connection_name=""):
    """bind an .interface.Interface() instance to connection_name"""
    global interfaces
    interfaces[connection_name] = interface


class InterfaceMessage(object):
    """this is a thin wrapper around all received interface messages"""
    def __init__(self, fields, raw_msg=None):
        """
        fields -- dict -- the original fields you passed to the Interface send method
        raw_msg -- mixed -- this is the raw message the interface returned
        """
        self.fields = fields
        self.raw_msg = raw_msg


class Interface(object):
    """base class for interfaces to messaging"""

    connected = False
    """true if a connection has been established, false otherwise"""

    connection_config = None
    """a config.Connection() instance"""

    def __init__(self, connection_config=None):
        self.connection_config = connection_config

    def create_msg(self, **kwargs):
        """create an interface message that is used to send/receive to the backend
        interface, this message is used to keep the api similar across the different
        methods and backends"""
        return InterfaceMessage(**kwargs)

    def _connect(self, connection_config): raise NotImplementedError()
    def connect(self, connection_config=None):
        """connect to the interface

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

    def get_connection(self): raise NotImplementedError()

    def _close(self): raise NotImplementedError()
    def close(self):
        """
        close an open connection
        """
        if not self.connected: return;

        self._close()
        self.connected = False
        self.log("Closed Connection")

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

        except Exception as e:
            self.raise_error(e)

    def _encode(self, fields):
        """prepare a message to be sent over the backend

        fields -- dict -- the fields to be converted to a string
        return -- string -- the message all ready to be sent
        """
        ret = pickle.dumps(fields, pickle.HIGHEST_PROTOCOL)
        key = self.connection_config.key
        if key:
            self.log("Message will be sent encrypted")
            # http://stackoverflow.com/questions/1220751/
            iv = Random.new().read(AES.block_size)
            aes = AES.new(key, AES.MODE_CFB, iv)
            ret = iv + aes.encrypt(ret)

        ret = base64.b64encode(ret)
        return ret

    def _decode(self, body):
        """this turns a message body back to the original fields

        body -- string -- the body to be converted to a dict
        return -- dict -- the fields of the original message
        """
        ret = base64.b64decode(body)
        key = self.connection_config.key
        if key:
            self.log("Message was sent encrypted")
            iv = ret[:AES.block_size]
            aes = AES.new(key, AES.MODE_CFB, iv)
            ret = aes.decrypt(ret[AES.block_size:])

        ret = pickle.loads(ret)
        return ret

    def _send(self, name, body, connection, **kwargs):
        """similar to self.send() but this takes a body, which is the message
        completely encoded and ready to be sent by the backend, instead of an
        interface_msg() instance"""
        raise NotImplementedError()

    def send(self, name, interface_msg, **kwargs):
        """send a message to message queue name

        name -- string -- the queue name
        interface_msg -- InterfaceMessage() -- an instance of InterfaceMessage, see self.create_msg()
        **kwargs -- dict -- anything else, this gets passed to self.connection()
        """
        if not interface_msg.fields:
            raise ValueError("the interface_msg has no fields to send")

        with self.connection(**kwargs) as connection:
            self._send(name, self._encode(interface_msg.fields), connection=connection)
            self.log("Message sent to {} -- {}", name, interface_msg.fields)

    def _count(self, name, connection, **kwargs): raise NotImplementedError()
    def count(self, name, **kwargs):
        """count how many messages are in queue name"""
        with self.connection(**kwargs) as connection:
            ret = int(self._count(name, connection=connection))
            return ret

    def _recv(self, name, connection, **kwargs):
        """return -- tuple -- (body, raw_msg) where body is the string of the
            message that needs to be decrypted, and raw_msg is the backend message
            object instance, this is returned because things like .ack() might need
            it to get an id or something"""
        raise NotImplementedError()

    def recv(self, name, timeout=None, **kwargs):
        """receive a message from queue name

        timeout -- integer -- seconds to try and receive a message before returning None
        return -- InterfaceMessage() -- an instance containing fields and raw_msg
        """
        with self.connection(**kwargs) as connection:
            interface_msg = None
            body, raw_msg = self._recv(
                name,
                connection=connection,
                timeout=timeout,
                **kwargs
            )
            if body:
                interface_msg = self.create_msg(fields=self._decode(body), raw_msg=raw_msg)
                self.log("Message received from {} -- {}", name, interface_msg.fields)

            return interface_msg

    def _ack(self, name, interface_msg, connection, **kwargs): raise NotImplementedError()
    def ack(self, name, interface_msg, **kwargs):
        """this will acknowledge that the interface message was received successfully"""
        with self.connection(**kwargs) as connection:
            self._ack(name, interface_msg, connection=connection)
            self.log("Message acked from {} -- {}", name, interface_msg.fields)

    def _clear(self, name, connection, **kwargs): raise NotImplementedError()
    def clear(self, name, **kwargs):
        """cliear the queue name"""
        with self.connection(**kwargs) as connection:
            self._clear(name, connection=connection)
            self.log("Messages cleared from {}", name)

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

