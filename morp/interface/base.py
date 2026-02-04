from contextlib import asynccontextmanager, AbstractAsyncContextManager
import json
from typing import Any, Self
from collections.abc import Mapping

from datatypes import Datetime, logging

try:
    from cryptography.fernet import Fernet
except ImportError:
    Fernet = None

from ..compat import *
from ..exception import InterfaceError
from ..config import Connection


logger = logging.getLogger(__name__)


class InterfaceABC(object):
    """This abstract base class containing all the methods that need to be
    implemented in a child interface class.

    Child classes should extend Interface (which extends this class). Interface
    contains the public API for using the interface, these methods are broken
    out from Interface just for convenience of seeing all the methods that must
    be implemented
    """
    async def _connect(self, connection_config: Connection) -> None:
        raise NotImplementedError()

    async def _get_connection(self) -> Any:
        """Returns a connection

        usually the body of this is as simple as `return self._connection` but
        needs to be implemented because the interfaces can be much more
        convoluted in how it creates and manages the connection

        :returns: Any, the interface connection instance
        """
        raise NotImplementedError()

    async def _close(self) -> None:
        raise NotImplementedError()

    async def _send(
        self,
        name: str,
        connection: Any,
        body: bytes,
        **kwargs,
    ) -> tuple[str, Any]:
        """similar to self.send() but this takes a body, which is the message
        completely encoded and ready to be sent by the backend

        :returns: tuple[str, Any], (_id, raw) where _id is the message unique
            id and raw is the returned receipt from the backend
        """
        raise NotImplementedError()

    async def _count(self, name: str, connection: Any, **kwargs) -> int:
        """count how many messages are currently in the queue

        :returns: int, the rough count, depending on the backend this might not
            be exact
        """
        raise NotImplementedError()

    async def _recv(
        self,
        name: str,
        connection: Any,
        **kwargs,
    ) -> tuple[str, bytes, Any]:
        """
        :returns: (_id, body, raw) where body is the body that
            was originally passed into send, raw is the untouched object
            returned from recv, and _id is the unique id of the message
        """
        raise NotImplementedError()

    async def _ack(
        self,
        name: str,
        connection: Any,
        fields: Mapping,
        **kwargs,
    ) -> None:
        raise NotImplementedError()

    async def _release(
        self,
        name: str,
        connection: Any,
        fields: Mapping,
        **kwargs,
    ) -> None:
        raise NotImplementedError()

    async def _clear(self, name: str, connection: Any, **kwargs) -> None:
        raise NotImplementedError()

    async def _delete(self, name: str, connection: Any, **kwargs) -> None:
        raise NotImplementedError()


class Interface(InterfaceABC):
    """base class for interfaces to messaging"""

    connected = False
    """true if a connection has been established, false otherwise"""

    connection_config = None
    """a config.Connection() instance"""

    def __init__(self, connection_config: Connection|None = None):
        self.connection_config = connection_config

    async def connect(self, connection_config: Connection|None = None) -> bool:
        """connect to the interface

        this will set the raw db connection to self.connection
        """
        if self.connected:
            return self.connected

        if connection_config:
            self.connection_config = connection_config

        try:
            self.connected = False
            await self._connect(self.connection_config)
            self.connected = True
            logger.debug("Connected to %s interface", self.__class__.__name__)

        except Exception as e:
            raise self._raise_error(e)

        return self.connected

    async def close(self) -> None:
        """
        close an open connection
        """
        if not self.connected:
            return;

        await self._close()
        self.connected = False
        logger.debug(
            "Closed Connection to %s interface", self.__class__.__name__,
        )

    @asynccontextmanager
    async def connection(
        self,
        connection: Any = None,
        **kwargs,
    ) -> AbstractAsyncContextManager:
        try:
            if not connection:
                if not self.connected:
                    await self.connect()

                connection = await self._get_connection()

            yield connection

        except Exception as e:
            self._raise_error(e)

    def _fields_to_body(self, fields: Mapping) -> bytes:
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
            if Fernet is None:
                logger.warning(
                    "Cannot encrypt because of missing dependencies",
                )

            else:
                logger.debug("Encrypting fields")
                f = Fernet(key)
                ret = f.encrypt(ret)

        return ret

    def _send_to_fields(self, _id: str, fields: Mapping, raw: Any) -> Mapping:
        """This creates the value that is returned from .send()

        :param _id: str, the unique id of the message, usually created by the
            backend
        :param fields: dict, the fields that were passed to .send()
        :param raw: mixed: whatever the backend returned after sending body
        :returns: dict: the fields populated with additional keys. If the key
            begins with an underscore then that usually means it was populated
            internally
        """
        fields["_id"] = _id
        fields["_raw_send"] = raw
        return fields

    async def send(self, name: str, fields: Mapping, **kwargs) -> Mapping:
        """send an interface message to the message queue

        :param name: str, the queue name
        :param fields: dict, the fields to send to the queue name
        :param **kwargs: anything else, this gets passed to self.connection()
        :returns: dict, see .send_to_fields() for what this returns
        """
        if not fields:
            raise ValueError("No fields to send")

        async with self.connection(**kwargs) as connection:
            kwargs["connection"] = connection
            _id, raw = await self._send(
                name=name,
                body=self._fields_to_body(fields),
                **kwargs
            )
            logger.debug(
                "Message %s sent to %s -- %s",
                _id,
                name,
                fields,
            )
            return self._send_to_fields(_id, fields, raw)

    async def count(self, name: str, **kwargs) -> int:
        """count how many messages are in queue name

        :returns: int, a rough count of the messages in the queue, this is
            backend dependent and might not be completely accurate
        """
        async with self.connection(**kwargs) as connection:
            kwargs["connection"] = connection
            return int(await self._count(name, **kwargs))

    def _body_to_fields(self, body: bytes) -> Mapping:
        """This will prepare the body returned from the backend to be passed
        to Message

        This turns a backend body back to the original fields

        :param body: bytes, the body returned from the backend that needs to be
            converted back into a dict
        :returns: dict, the fields of the original message
        """
        key = self.connection_config.key
        if key:
            if Fernet is None:
                logger.warning(
                    "Skipping decrypt because of missing dependencies",
                )
                ret = body

            else:
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

    def _recv_to_fields(self, _id: str, body: bytes, raw: Any) -> Mapping:
        """This creates the value that is returned from .recv()

        :param _id: str, the unique id of the message, usually created by the
            backend
        :param body: bytes, the backend message body
        :param raw: mixed: whatever the backend fetched from its receive method
        :returns: dict: the fields populated with additional keys. If the key
            begins with an underscore then that usually means it was populated
            internally
        """
        fields = self._body_to_fields(body)
        fields["_id"] = _id
        fields["_raw_recv"] = raw
        fields.setdefault("_count", 0)
        fields["_count"] += 1
        return fields

    async def recv(
        self,
        name: str,
        timeout: float|int = 0.0,
        **kwargs,
    ) -> Mapping:
        """receive a message from queue name

        :param name: str, the queue name
        :param timeout: integer, seconds to try and receive a message before
            returning None, 0 means no timeout or max timeout if the interface
            requires a timeout
        :returns: dict, the fields that were sent via .send populated with
            additional keys (additional keys will usually be prefixed with an
            underscore), it will return None if it failed to fetch (ie, timeout
            or error)
        """
        async with self.connection(**kwargs) as connection:
            kwargs["connection"] = connection
            _id, body, raw = await self._recv(
                name,
                timeout=timeout,
                **kwargs
            )
            fields = self._recv_to_fields(_id, body, raw) if body else None
            if fields:
                logger.log_for(
                    debug=(
                        "Message %s received from %s -- %s",
                        _id,
                        name,
                        fields
                    ),
                    info=(
                        "Message %s recceived from %s -- %s",
                        _id,
                        name,
                        fields.keys(),
                    ),
                )

            return fields

    async def ack(self, name: str, fields: Mapping, **kwargs) -> None:
        """this will acknowledge that the interface message was received
        successfully

        :param name: str, the queue name
        :param fields: dict, these are the fields returned from .recv that have
            additional fields that the backend will most likely need to ack the
            message
        """
        async with self.connection(**kwargs) as connection:
            kwargs["connection"] = connection
            await self._ack(name, fields=fields, **kwargs)
            logger.debug("Message %s acked from %s", fields["_id"], name)

    async def release(self, name: str, fields: Mapping, **kwargs) -> None:
        """release the message back into the queue, this is usually for when
        processing the message has failed and so a new attempt to process the
        message should be made

        :param name: str, the queue name
        :param fields: dict, these are the fields returned from .recv that have
            additional fields that the backend will most likely need to release
            the message
        """
        async with self.connection(**kwargs) as connection:
            kwargs["connection"] = connection
            delay_seconds = max(kwargs.pop('delay_seconds', 0), 0)
            count = fields.get("_count", 0)

            if delay_seconds == 0:
                if count:
                    max_timeout = self.connection_config.options.get(
                        "max_timeout"
                    )
                    backoff = self.connection_config.options.get(
                        "backoff_multiplier"
                    )
                    amplifier = self.connection_config.options.get(
                        "backoff_amplifier",
                        count
                    )
                    delay_seconds = min(
                        max_timeout,
                        (count * backoff) * amplifier
                    )

            await self._release(
                name,
                fields=fields,
                delay_seconds=delay_seconds,
                **kwargs
            )
            logger.debug(
                "Message %s released back to %s count %s, with delay %ss",
                fields["_id"],
                name,
                count,
                delay_seconds,
            )

    async def unsafe_clear(self, name: str, **kwargs) -> None:
        """clear the queue name, clearing the queue removes all the messages
        from the queue but doesn't delete the actual queue

        :param name: str, the queue name to clear
        """
        async with self.connection(**kwargs) as connection:
            kwargs["connection"] = connection
            await self._clear(name, **kwargs)
            logger.debug("Messages cleared from %s", name)

    async def unsafe_delete(self, name: str, **kwargs) -> None:
        """delete the queue, this removes messages and the queue

        :param name: str, the queue name to delete
        """
        async with self.connection(**kwargs) as connection:
            kwargs["connection"] = connection
            await self._delete(name, **kwargs)
            logger.debug("Queue %s deleted", name)

    def _raise_error(self, e: BaseException) -> None:
        """this is just a wrapper to make the passed in exception an
        InterfaceError"""
        if (
            isinstance(e, InterfaceError)
            or hasattr(builtins, e.__class__.__name__)
        ):
            raise e

        else:
            raise InterfaceError(e) from e

