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


class InterfaceABC(LogMixin):
    """This abstract base class containing all the methods that need to be implemented
    in a child interface class.

    Child classes should extend Interface (which extends this class). Interface
    contains the public API for using the interface, these methods are broken out
    from Interface just for convenience of seeing all the methods that must be
    implemented
    """
    def _connect(self, connection_config): raise NotImplementedError()

    def get_connection(self): raise NotImplementedError()

    def _close(self): raise NotImplementedError()

    def _send(self, name, connection, body, **kwargs):
        """similar to self.send() but this takes a body, which is the message
        completely encoded and ready to be sent by the backend

        :returns: tuple, (_id, raw) where _id is the message unique id and raw is
            the returned receipt from the backend
        """
        raise NotImplementedError()

    def _count(self, name, connection, **kwargs):
        """count how many messages are currently in the queue

        :returns: int, the rough count, depending on the backend this might not be exact
        """
        raise NotImplementedError()

    def _recv(self, name, connection, **kwargs):
        """
        :returns: tuple, (_id, body, raw) where body is the body that was originally
            passed into send, raw is the untouched object returned from recv, and
            _id is the unique id of the message
        """
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

    def __init__(self, connection_config=None):
        self.connection_config = connection_config

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

        :param fields: dict, all the fields that will be sent to the backend
        :returns: bytes, the fully encoded fields
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

        return ret

    def send_to_fields(self, _id, fields, raw):
        """This creates the value that is returned from .send()

        :param _id: str, the unique id of the message, usually created by the backend
        :param fields: dict, the fields that were passed to .send()
        :param raw: mixed: whatever the backend returned after sending body
        :returns: dict: the fields populated with additional keys. If the key begins
            with an underscore then that usually means it was populated internally
        """
        fields["_id"] = _id
        fields["_send_raw"] = raw
        return fields

    def send(self, name, fields, **kwargs):
        """send an interface message to the message queue

        :param name: str, the queue name
        :param fields: dict, the fields to send to the queue name
        :param **kwargs: anything else, this gets passed to self.connection()
        :returns: dict, see .send_to_fields() for what this returns
        """
        if not fields:
            raise ValueError("No fields to send")

        with self.connection(**kwargs) as connection:
            _id, raw = self._send(
                name=name,
                connection=connection,
                body=self.fields_to_body(fields),
                **kwargs
            )
            self.log(f"Message {_id} sent to {name} -- {fields}")
            return self.send_to_fields(_id, fields, raw)

    def count(self, name, **kwargs):
        """count how many messages are in queue name

        :returns: int, a rough count of the messages in the queue, this is backend
            dependent and might not be completely accurate
        """
        with self.connection(**kwargs) as connection:
            return int(self._count(name, connection=connection))

    def body_to_fields(self, body):
        """This will prepare the body returned from the backend to be passed
        to Message

        This turns a backend body back to the original fields

        :param body: bytes, the body returned from the backend that needs to be converted
            back into a dict
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
            ret = pickle.loads(ret)

        elif serializer == "json":
            ret = json.loads(ret)

        return ret

    def recv_to_fields(self, _id, body, raw):
        """This creates the value that is returned from .recv()

        :param _id: str, the unique id of the message, usually created by the backend
        :param body: bytes, the backend message body
        :param raw: mixed: whatever the backend fetched from its receive method
        :returns: dict: the fields populated with additional keys. If the key begins
            with an underscore then that usually means it was populated internally
        """
        fields = self.body_to_fields(body)
        fields["_id"] = _id
        fields["_raw"] = raw
        fields["_count"] = 1
        return fields

    def recv(self, name, timeout=None, **kwargs):
        """receive a message from queue name

        :param name: str, the queue name
        :param timeout: integer, seconds to try and receive a message before returning None
        :returns: dict, the fields that were sent via .send populated with additional
            keys (additional keys will usually be prefixed with an underscore), it
            will return None if it failed to fetch (ie, timeout or error)
        """
        with self.connection(**kwargs) as connection:
            _id, body, raw = self._recv(
                name,
                connection=connection,
                timeout=timeout,
                **kwargs
            )
            return self.recv_to_fields(_id, body, raw) if body else None

    def ack(self, name, fields, **kwargs):
        """this will acknowledge that the interface message was received successfully

        :param name: str, the queue name
        :param fields: dict, these are the fields returned from .recv that have
            additional fields that the backend will most likely need to ack the message
        """
        with self.connection(**kwargs) as connection:
            self._ack(name, connection=connection, fields=fields)
            self.log("Message {} acked from {}", fields["_id"], name)

    def release(self, name, fields, **kwargs):
        """release the message back into the queue, this is usually for when processing
        the message has failed and so a new attempt to process the message should be made

        :param name: str, the queue name
        :param fields: dict, these are the fields returned from .recv that have
            additional fields that the backend will most likely need to release the message
        """
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
        """cliear the queue name, clearing the queue removes all the messages from
        the queue but doesn't delete the actual queue

        :param name: str, the queue name to clear
        """
        with self.connection(**kwargs) as connection:
            self._clear(name, connection=connection)
            self.log("Messages cleared from {}", name)

    def unsafe_delete(self, name, **kwargs):
        """delete the queue, this removes messages and the queue

        :param name: str, the queue name to delete
        """
        with self.connection(**kwargs) as connection:
            self._delete(name, connection=connection)
            self.log("Queue {} deleted", name)

    def raise_error(self, e):
        """this is just a wrapper to make the passed in exception an InterfaceError"""
        if isinstance(e, InterfaceError) or hasattr(builtins, e.__class__.__name__):
            raise e

        else:
            raise InterfaceError(e) from e

