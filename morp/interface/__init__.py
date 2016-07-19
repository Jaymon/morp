import logging
import sys
from contextlib import contextmanager
import base64
import datetime

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
    """this is a thin wrapper around all received interface messages

    An instance of this class exposes these properties:
        .fields -- contain what will be passed to the backend interface, but also 
        ._count -- how many times this message has been received from the backend interface
        ._created -- when this message was first sent
    """
    @property
    def body(self):
        """Return the body of the current internal fields"""
        d = self.depart()
        return self._encode(d)

    @body.setter
    def body(self, b):
        """this will take a body and convert it to fields"""
        d = self._decode(b)
        self.update(fields=d)

    def __init__(self, interface, raw):
        """
        interface -- Interface -- the specific interface to send/receive messages
        raw -- mixed -- the raw message the interface returned
        """
        self.fields = {} # the original fields you passed to the Interface send method
        self.interface = interface
        self.raw = raw
        self.update()

    def depart(self):
        """whatever is returned from this method is serialized and placed in the body
        of the message that is actually sent through the interface. This can return
        anything since it is serialized but you will probably need to mess with 
        populate() also since the default implementations expect dicts.

        return -- mixed -- anything you want to send in the message in the form you
            want to send it
        """
        return {
            "fields": self.fields,
            "_count": self._count + 1,
            "_created": self._created if self._created else datetime.datetime.utcnow()
        }

    def populate(self, fields):
        """when a message is read from the interface, the unserialized "fields" of
        the returned body will pass through this message.

        fields -- mixed -- the body, unserialized, read from the backend interface
        """
        if not fields: fields = {}
        self.fields = fields.get("fields", fields)
        self._count = fields.get("_count", 0)
        self._created = fields.get("_created", None)

    def update(self, fields=None, body=""):
        """this is the public wrapper around populate(), usually, when you want to 
        customize functionality you would override populate() and depart() and leave
        this method alone"""
        # we call this regardless to set defaults
        self.populate(fields)

        if body:
            # this will override any fields (and defaults) that were set in populate
            self.body = body

    def _encode(self, fields):
        """prepare a message to be sent over the backend

        fields -- dict -- the fields to be converted to a string
        return -- string -- the message all ready to be sent
        """
        ret = pickle.dumps(fields, pickle.HIGHEST_PROTOCOL)
        key = self.interface.connection_config.key
        if key:
            logger.debug("Encrypting fields")
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
        key = self.interface.connection_config.key
        if key:
            logger.debug("Decoding encrypted body")
            iv = ret[:AES.block_size]
            aes = AES.new(key, AES.MODE_CFB, iv)
            ret = aes.decrypt(ret[AES.block_size:])

        ret = pickle.loads(ret)
        return ret


class Interface(object):
    """base class for interfaces to messaging"""

    connected = False
    """true if a connection has been established, false otherwise"""

    connection_config = None
    """a config.Connection() instance"""

    message_class = InterfaceMessage
    """the interface message class that is used to send/receive the actual messages,
    this is different than the message.Message classes, see .create_msg()"""

    def __init__(self, connection_config=None):
        self.connection_config = connection_config

    def create_msg(self, fields=None, body=None, raw=None):
        """create an interface message that is used to send/receive to the backend
        interface, this message is used to keep the api similar across the different
        methods and backends"""
        interface_msg = self.message_class(interface=self, raw=raw)
        interface_msg.update(fields=fields, body=body)
        return interface_msg

    def _connect(self, connection_config): raise NotImplementedError()
    def connect(self, connection_config=None):
        """connect to the interface

        this will set the raw db connection to self.connection
        """

        if self.connected: return self.connected
        if connection_config: self.connection_config = connection_config

        self.connection_config.options.setdefault('max_timeout', 3600) # 1 hour to process message
        self.connection_config.options.setdefault('backoff_multiplier', 5) # failure backoff multiplier

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
            self._send(name, interface_msg.body, connection=connection)
            self.log("Message sent to {} -- {}", name, interface_msg.fields)

    def _count(self, name, connection, **kwargs): raise NotImplementedError()
    def count(self, name, **kwargs):
        """count how many messages are in queue name"""
        with self.connection(**kwargs) as connection:
            ret = int(self._count(name, connection=connection))
            return ret

    def _recv(self, name, connection, **kwargs):
        """return -- tuple -- (body, raw) where body is the string of the
            message that needs to be decrypted, and raw is the backend message
            object instance, this is returned because things like .ack() might need
            it to get an id or something"""
        raise NotImplementedError()

    def recv(self, name, timeout=None, **kwargs):
        """receive a message from queue name

        timeout -- integer -- seconds to try and receive a message before returning None
        return -- InterfaceMessage() -- an instance containing fields and raw
        """
        with self.connection(**kwargs) as connection:
            interface_msg = None
            body, raw = self._recv(
                name,
                connection=connection,
                timeout=timeout,
                **kwargs
            )
            if body:
                interface_msg = self.create_msg(body=body, raw=raw)
                self.log("Message received from {} -- {}", name, interface_msg.fields)

            return interface_msg

    def _release(self, name, interface_msg, connection, **kwargs): raise NotImplementedError()
    def release(self, name, interface_msg, **kwargs):
        """release the message back into the queue, this is usually for when processing
        the message has failed and so a new attempt to process the message should be made"""
        #interface_msg.raw.load()
        with self.connection(**kwargs) as connection:
            delay_seconds = 0

            cnt = interface_msg._count
            if cnt:
                max_timeout = self.connection_config.options.get("max_timeout")
                backoff = self.connection_config.options.get("backoff_multiplier")
                delay_seconds = min(
                    max_timeout,
                    (cnt * backoff) * cnt
                )

            self._release(name, interface_msg, connection=connection, delay_seconds=delay_seconds)
            self.log(
                "Message released back to {} count {}, with delay {}s",
                name,
                interface_msg._count,
                delay_seconds
            )

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

