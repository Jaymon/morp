# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import logging
import sys
from contextlib import contextmanager
import base64
import datetime

from cryptography.fernet import Fernet
import dsnparse

from ..compat import *
from ..config import DsnConnection
from ..exception import InterfaceError


logger = logging.getLogger(__name__)


interfaces = {}
"""holds all configured interfaces"""


def get_interfaces():
    global interfaces
    if not interfaces:
        configure_environ()
    return interfaces


def get_interface(connection_name=""):
    """get the configured interface that corresponds to connection_name"""
    global interfaces
    if not interfaces:
        configure_environ()
    return interfaces[connection_name]


def set_interface(interface, connection_name=""):
    """bind an .interface.Interface() instance to connection_name"""
    global interfaces
    interfaces[connection_name] = interface


def find_environ(dsn_env_name='MORP_DSN', connection_class=DsnConnection):
    """Returns Connection instances found in the environment

    configure interfaces based on environment variables

    by default, when morp is imported, it will look for MORP_DSN, and MORP_DSN_N (where
    N is 1 through infinity) in the environment, if it finds them, it will assume they
    are dsn urls that morp understands and will configure connections with them. If you
    don't want this behavior (ie, you want to configure morp manually) then just make sure
    you don't have any environment variables with matching names

    The num checks (eg MORP_DSN_1, MORP_DSN_2) go in order, so you can't do MORP_DSN_1, MORP_DSN_3,
    because it will fail on _2 and move on, so make sure your num dsns are in order (eg, 1, 2, 3, ...)

    :param dsn_env_name: str, the environment variable name to find the DSN, this
        will aslo check for values of <dsn_env_name>_N where N is 1 -> N, so you
        can configure multiple DSNs in the environment and this will pick them all
        up
    :param connection_class: Connection, the class that will receive the dsn values
    :returns: generator<connection_class>
    """
    return dsnparse.parse_environs(dsn_env_name, parse_class=connection_class)


def configure_environ():
    """auto hook to configure the environment"""
    for c in find_environ():
        inter = c.interface
        set_interface(c.interface, c.name)


def configure(dsn, connection_class=DsnConnection):
    """
    configure an interface to be used to send messages to a backend

    you use this function to configure an Interface using a dsn, then you can get
    that interface using the get_interface() method

    :param dsn: string, a properly formatted morp dsn, see DsnConnection for how to format the dsn
    """
    #global interfaces
    c = dsnparse.parse(dsn, parse_class=connection_class)
    set_interface(c.interface, c.name)


class InterfaceMessage(object):
    """this is a thin wrapper around all received interface messages

    An instance of this class exposes these properties:
        .fields -- contain what will be passed to the backend interface, but also 
        ._count -- how many times this message has been received from the backend interface
        ._created -- when this message was first sent
    """
    @property
    def _id(self):
        raise NotImplementedError()

#     @property
#     def body(self):
#         """Return the body of the current internal fields"""
#         d = self.depart()
#         return self._encode(d)
# 
#     @body.setter
#     def body(self, b):
#         """this will take a body and convert it to fields"""
#         d = self._decode(b)
#         self.update(fields=d)

    def __init__(self, name, interface, fields=None, raw=None):
        """
        interface -- Interface -- the specific interface to send/receive messages
        raw -- mixed -- the raw message the interface returned
        """
        self.name = name
        self.fields = fields or {} # the original fields you passed to the Interface send method
        self.interface = interface
        self.raw = raw
        self.body = None
        #self.update()

#     def send(self):
#         return self.interface.send(self.name, self)
# 
#     def ack(self):
#         return self.interface.ack(self.name, self)

    def to_interface(self):
        return self._encode(self.fields)

    def from_interface(self, body):
        self.body = body
        fields = self._decode(body)
        for k, v in fields.items():
            self.fields[k] = v
        #self.fields = fields or {}

#     def depart(self):
#         """whatever is returned from this method is serialized and placed in the body
#         of the message that is actually sent through the interface. This can return
#         anything since it is serialized but you will probably need to mess with 
#         populate() also since the default implementations expect dicts.
# 
#         return -- mixed -- anything you want to send in the message in the form you
#             want to send it
#         """
#         return {
#             "fields": self.fields,
#             "_count": self._count + 1,
#             "_created": self._created if self._created else datetime.datetime.utcnow()
#         }

#     def populate(self, fields):
#         """when a message is read from the interface, the unserialized "fields" of
#         the returned body will pass through this message.
# 
#         fields -- mixed -- the body, unserialized, read from the backend interface
#         """
#         if not fields: fields = {}
#         self.fields = fields.get("fields", fields)
#         self._count = fields.get("_count", 0)
#         self._created = fields.get("_created", None)
# 
#     def update(self, fields=None, body=""):
#         """this is the public wrapper around populate(), usually, when you want to 
#         customize functionality you would override populate() and depart() and leave
#         this method alone"""
#         # we call this regardless to set defaults
#         self.populate(fields)
# 
#         if body:
#             # this will override any fields (and defaults) that were set in populate
#             self.body = body

    def _encode(self, fields):
        """prepare a message to be sent over the backend

        fields -- dict -- the fields to be converted to a string
        return -- string -- the message all ready to be sent
        """
        ret = pickle.dumps(fields, pickle.HIGHEST_PROTOCOL)
        key = self.interface.connection_config.key
        if key:
            logger.debug("Encrypting fields")
            f = Fernet(key)
            ret = String(f.encrypt(ByteString(ret)))

        else:
            ret = String(base64.b64encode(ret))

        return ret

    def _decode(self, body):
        """this turns a message body back to the original fields

        body -- string -- the body to be converted to a dict
        return -- dict -- the fields of the original message
        """
        key = self.interface.connection_config.key
        if key:
            logger.debug("Decoding encrypted body")
            f = Fernet(key)
            ret = f.decrypt(ByteString(body))

        else:
            ret = base64.b64decode(body)

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

    def create_imessage(self, name, fields=None, body=None, raw=None):
        """create an interface message that is used to send/receive to the backend
        interface, this message is used to keep the api similar across the different
        methods and backends"""
        if fields:
            imessage = self.message_class(name=name, interface=self, fields=fields)

        elif body:
            imessage = self.message_class(name=name, interface=self, raw=raw)
            imessage.from_interface(body)

        else:
            raise ValueError("Could not create imessage")

        return imessage

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

    def _send(self, name, connection, body, **kwargs):
        """similar to self.send() but this takes a body, which is the message
        completely encoded and ready to be sent by the backend, instead of an
        interface_msg() instance"""
        raise NotImplementedError()

    def send(self, name, fields, **kwargs):
        """send an interface message to the message queue

        :param name: str, the queue name
        :param fields: dict, the fields to send to the queue name
        :param **kwargs: anything else, this gets passed to self.connection()
        :returns: InterfaceMessage instance, see self.create_imessage()
        """
        if not fields:
            raise ValueError("No fields to send")

        imessage = self.create_imessage(name=name, fields=fields)

        with self.connection(**kwargs) as connection:
            self._send(
                name=imessage.name,
                connection=connection,
                body=imessage.to_interface(),
                **kwargs
            )
            self.log(f"Message sent to {imessage.name} -- {imessage.fields}")

        return imessage

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
            imessage = None
            body, raw = self._recv(
                name,
                connection=connection,
                timeout=timeout,
                **kwargs
            )
            if body:
                imessage = self.create_imessage(name=name, body=body, raw=raw)
                self.log(
                    "Message {} received from {} -- {}",
                    imessage._id,
                    name,
                    imessage.fields
                )

            return imessage

    def _ack(self, name, connection, imessage, **kwargs): raise NotImplementedError()
    def ack(self, name, imessage, **kwargs):
        """this will acknowledge that the interface message was received successfully"""
        with self.connection(**kwargs) as connection:
            self._ack(name, connection=connection, imessage=imessage)
            self.log("Message {} acked from {}", imessage._id, name)

    def _release(self, name, connection, imessage, **kwargs): raise NotImplementedError()
    def release(self, name, imessage, **kwargs):
        """release the message back into the queue, this is usually for when processing
        the message has failed and so a new attempt to process the message should be made"""
        #interface_msg.raw.load()
        with self.connection(**kwargs) as connection:
            delay_seconds = max(kwargs.get('delay_seconds', 0), 0)

            if delay_seconds == 0:
                cnt = imessage._count
                if cnt:
                    max_timeout = self.connection_config.options.get("max_timeout")
                    backoff = self.connection_config.options.get("backoff_multiplier")
                    delay_seconds = min(
                        max_timeout,
                        (cnt * backoff) * cnt
                    )

            self._release(name, connection=connection, imessage=imessage, delay_seconds=delay_seconds)
            self.log(
                "Message {} released back to {} count {}, with delay {}s",
                imessage._id,
                name,
                imessage._count,
                delay_seconds
            )

    def _clear(self, name, connection, **kwargs): raise NotImplementedError()
    def unsafe_clear(self, name, **kwargs):
        """cliear the queue name"""
        with self.connection(**kwargs) as connection:
            self._clear(name, connection=connection)
            self.log("Messages cleared from {}", name)

    def _delete(self, name, connection, **kwargs): raise NotImplementedError()
    def unsafe_delete(self, name, **kwargs):
        with self.connection(**kwargs) as connection:
            self._delete(name, connection=connection)
            self.log("Queue {} deleted", name)

    def log(self, format_str, *format_args, **log_options):
        """
        wrapper around the module's logger

        format_str -- string -- the message to log
        *format_args -- list -- if format_str is a string containing {}, then format_str.format(*format_args) is ran
        **log_options --
        level -- something like logging.DEBUG
        """
        log_level = getattr(logging, log_options.get('level', "DEBUG").upper())
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

    def raise_error(self, e):
        """this is just a wrapper to make the passed in exception an InterfaceError"""

        if isinstance(e, InterfaceError) or hasattr(builtins, e.__class__.__name__):
            raise e

        else:
            raise InterfaceError(e) from e

