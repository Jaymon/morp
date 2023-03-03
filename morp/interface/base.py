# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import logging
import sys
from contextlib import contextmanager
import base64
import datetime
import json

from datatypes import LogMixin, Datetime
from cryptography.fernet import Fernet
import dsnparse

from ..compat import *
from ..config import DsnConnection
from ..exception import InterfaceError


logger = logging.getLogger(__name__)


class InterfaceMessage(object):
    """this is a thin wrapper around all received interface messages

    An instance of this class could expose these properties:
        .fields -- contain what will be passed to the backend interface, but also 
        ._count -- how many times this message has been received from the backend interface
        ._created -- when this message was first sent
    """
    @property
    def _id(self):
        """Sub-classes customized for the interface should flesh this out"""
        raise NotImplementedError()

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

    def to_interface(self):
        """This will prepare the fields passed from Message to Interface.send

        :returns: str, the fully encoded fields
        """
        return self._encode(self.fields)

    def from_interface(self, body):
        """This will prepare the body returned from Interface.recv to be passed
        to Message

        :param body: str, the body returned from the interface
        """
        self.body = body
        self.fields.update(self._decode(body))

    def _encode(self, fields):
        """prepare a message to be sent over the backend

        This base64 encodes the non-encrypted pickled body just for encoding simplicity

        :param fields: dict, the fields to be converted to a string
        :returns: str, the message all ready to be sent
        """
        serializer = self.interface.connection_config.serializer
        if serializer == "pickle":
            ret = pickle.dumps(fields, pickle.HIGHEST_PROTOCOL)

        elif serializer == "json":
            ret = ByteString(json.dumps(fields))

        key = self.interface.connection_config.key
        if key:
            logger.debug("Encrypting fields")
            f = Fernet(key)
            ret = f.encrypt(ret)

#         else:
#             if serializer == "pickle":
#                 ret = String(base64.b64encode(ret))

        return ret

    def _decode(self, body):
        """this turns a message body back to the original fields

        :param body: str, the body to be converted to a dict
        :returns: dict, the fields of the original message
        """
        key = self.interface.connection_config.key
        if key:
            logger.debug("Decoding encrypted body")
            f = Fernet(key)
            ret = f.decrypt(ByteString(body))

        else:
            ret = body

        serializer = self.interface.connection_config.serializer
        if serializer == "pickle":
            #ret = base64.b64decode(ret)
            ret = pickle.loads(ret)

        elif serializer == "json":
            ret = json.loads(ret)

        return ret



class InterfaceABC(LogMixin):
    def _connect(self, connection_config): raise NotImplementedError()

    def get_connection(self): raise NotImplementedError()

    def _close(self): raise NotImplementedError()

    def _send(self, name, connection, body, **kwargs):
        """similar to self.send() but this takes a body, which is the message
        completely encoded and ready to be sent by the backend, instead of an
        interface_msg() instance"""
        raise NotImplementedError()

    def _count(self, name, connection, **kwargs): raise NotImplementedError()

    def _recv(self, name, connection, **kwargs):
        """return -- tuple -- (body, raw) where body is the string of the
            message that needs to be decrypted, and raw is the backend message
            object instance, this is returned because things like .ack() might need
            it to get an id or something"""
        raise NotImplementedError()

    def _ack(self, name, connection, fields, **kwargs): raise NotImplementedError()
    def _release(self, name, connection, fields, **kwargs): raise NotImplementedError()
    def _clear(self, name, connection, **kwargs): raise NotImplementedError()
    def _delete(self, name, connection, **kwargs): raise NotImplementedError()


class Interface(InterfaceABC):
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
            self.log(f"Connected to {self.__class__.__name__} interface")

        except Exception as e:
            raise self.raise_error(e)

        return self.connected

    def close(self):
        """
        close an open connection
        """
        if not self.connected: return;

        self._close()
        self.connected = False
        self.log(f"Closed Connection to {self.__class__.__name__} interface")

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

    def fields_to_body(self, fields):
        """This will prepare the fields passed from Message to Interface.send


        prepare a message to be sent over the backend

        This base64 encodes the non-encrypted pickled body just for encoding simplicity

        :param fields: dict, the fields to be converted to a string
        :returns: str, the message all ready to be sent
        :returns: str, the fully encoded fields
        """
        serializer = self.connection_config.serializer
        if serializer == "pickle":
            ret = pickle.dumps(fields, pickle.HIGHEST_PROTOCOL)

        elif serializer == "json":
            ret = ByteString(json.dumps(fields))

        key = self.connection_config.key
        if key:
            logger.debug("Encrypting fields")
            f = Fernet(key)
            ret = f.encrypt(ret)

#         else:
#             if serializer == "pickle":
#                 ret = String(base64.b64encode(ret))

        return ret

    def send_to_fields(self, _id, fields, raw):
        fields["_id"] = _id
        fields["_send_raw"] = raw
        return fields

    def send(self, name, fields, **kwargs):
        """send an interface message to the message queue

        :param name: str, the queue name
        :param fields: dict, the fields to send to the queue name
        :param **kwargs: anything else, this gets passed to self.connection()
        :returns: InterfaceMessage instance, see self.create_imessage()
        """
        if not fields:
            raise ValueError("No fields to send")

        #imessage = self.create_imessage(name=name, fields=fields)

        with self.connection(**kwargs) as connection:
            _id, raw = self._send(
                name=name,
                connection=connection,
                body=self.fields_to_body(fields),
                **kwargs
            )
            self.log(f"Message {_id} sent to {name} -- {fields}")
            return self.send_to_fields(_id, fields, raw)

        #return imessage

    def count(self, name, **kwargs):
        """count how many messages are in queue name"""
        with self.connection(**kwargs) as connection:
            ret = int(self._count(name, connection=connection))
            return ret

    def body_to_fields(self, body):
        """This will prepare the body returned from Interface.recv to be passed
        to Message

        his turns a message body back to the original fields

        :param body: str, the body returned from the interface
        :param body: str, the body to be converted to a dict
        :returns: dict, the fields of the original message
        """
        key = self.connection_config.key
        if key:
            logger.debug("Decoding encrypted body")
            f = Fernet(key)
            ret = f.decrypt(ByteString(body))

        else:
            ret = body

        serializer = self.connection_config.serializer
        if serializer == "pickle":
            #ret = base64.b64decode(ret)
            ret = pickle.loads(ret)

        elif serializer == "json":
            ret = json.loads(ret)

        return ret

    def recv_to_fields(self, _id, body, raw):
        fields = self.body_to_fields(body)
        fields["_id"] = _id
        fields["_raw"] = raw
        fields["_count"] = 0
        fields["_created"] = Datetime()
        return fields

    def recv(self, name, timeout=None, **kwargs):
        """receive a message from queue name

        timeout -- integer -- seconds to try and receive a message before returning None
        return -- InterfaceMessage() -- an instance containing fields and raw
        """
        with self.connection(**kwargs) as connection:
#             fields = {}
#             imessage = None
            _id, body, raw = self._recv(
                name,
                connection=connection,
                timeout=timeout,
                **kwargs
            )
            return self.recv_to_fields(_id, body, raw) if body else {}


#             if body:
#                 fields = self.from_interface(body)
# #                 self.log(
# #                     f"Message received from {name} -- {fields}",
# #                     imessage._id,
# #                     name,
# #                     imessage.fields
# #                 )
# 
#                 pout.v(raw.message_id)
#                 fields["_raw"] = raw
# 
#             return fields

#                 imessage = self.create_imessage(name=name, body=body, raw=raw)
#                 self.log(
#                     "Message {} received from {} -- {}",
#                     imessage._id,
#                     name,
#                     imessage.fields
#                 )
# 
#             return imessage

    def ack(self, name, fields, **kwargs):
        """this will acknowledge that the interface message was received successfully"""
        with self.connection(**kwargs) as connection:
            self._ack(name, connection=connection, fields=fields)
            self.log("Message {} acked from {}", fields["_id"], name)

    def release(self, name, fields, **kwargs):
        """release the message back into the queue, this is usually for when processing
        the message has failed and so a new attempt to process the message should be made"""
        #interface_msg.raw.load()
        with self.connection(**kwargs) as connection:
            delay_seconds = max(kwargs.get('delay_seconds', 0), 0)
            count = fields.get("_count", 0)

            if delay_seconds == 0:
                if count:
                    max_timeout = self.connection_config.options.get("max_timeout")
                    backoff = self.connection_config.options.get("backoff_multiplier")
                    amplifier = self.connection_config.options.get("backoff_amplifier", count)
                    delay_seconds = min(
                        max_timeout,
                        (count * backoff) * amplifier
                    )

            self._release(name, connection=connection, fields=fields, delay_seconds=delay_seconds)
            self.log(
                "Message {} released back to {} count {}, with delay {}s",
                fields["_id"],
                name,
                count,
                delay_seconds
            )

    def unsafe_clear(self, name, **kwargs):
        """cliear the queue name"""
        with self.connection(**kwargs) as connection:
            self._clear(name, connection=connection)
            self.log("Messages cleared from {}", name)

    def unsafe_delete(self, name, **kwargs):
        with self.connection(**kwargs) as connection:
            self._delete(name, connection=connection)
            self.log("Queue {} deleted", name)

    def raise_error(self, e):
        """this is just a wrapper to make the passed in exception an InterfaceError"""

        if isinstance(e, InterfaceError) or hasattr(builtins, e.__class__.__name__):
            raise e

        else:
            raise InterfaceError(e) from e

