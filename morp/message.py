from contextlib import asynccontextmanager, AbstractAsyncContextManager
import logging
import inspect
import typing

from datatypes import (
    ReflectClass,
    ReflectName,
    ReflectType,
    make_dict,
    classproperty,
)

from .compat import *
from .interface import get_interface
from .interface.base import Interface
from .exception import ReleaseMessage, AckMessage
from .config import environ


logger = logging.getLogger(__name__)


class Message(object):
    """
    this is the base class for sending and handling a message

    to add a new message to your application, just subclass this class

    :example:
        import morp

        class CustomMessage(morp.Message):
            # message fields
            foo: int
            bar: str

            # fields that aren't part of the message start with an underscore
            _ignored: bool = False

            async def handle(self):
                # This will be called when the message is consumed using
                # the CustomMessage.handle method
                pass

        m1 = CustomMessage(foo=1, bar="one")
        await m1.send() # m1 is sent with foo and bar fields

        m2 = await CustomMessage.create(foo=2, bar="two")
        # m2 was created and sent with foo and bar fields

        await CustomMessage.process(2)
        # both m1 and m2 were consumed and their `.handle` methods called

    By default, all subclasses will go to the same queue and then when the
    queue is consumed the correct child class will be created and consume the
    message with its `.handle` method.

    If you would like your subclass to use a different queue then just set the
    `.queue_name` property on the class and it will use a different queue
    """
    queue_name: typing.ClassVar[str] = "morp-messages"
    """The queue name, see .get_name()"""

    _connection_name: typing.ClassVar[str] = ""
    """the name of the connection to use to retrieve the interface"""

    _classpath_key: typing.ClassVar[str] = "_classpath"
    """The key that will be used to hold the Message's child class's full
    classpath, see `._to_interface` and `.from_interface`"""

    _message_classes: typing.ClassVar[dict[str, type[typing.Self]]] = {}
    """Holds all the children message classes. See `__init_subclass__`"""

    schema: typing.ClassVar[dict[str, ReflectType]|None] = None
    """This is dynamically set in `.__init_subclass__`"""

    @classproperty
    def interface(cls) -> Interface:
        return get_interface(cls._connection_name)

    @property
    def fields(self) -> dict:
        fields = {}

        for field_name in self.schema.keys():
            v = getattr(self, field_name, typing.NoReturn)
            if v is not typing.NoReturn:
                fields[field_name] = v

        return fields

    def __init__(self, fields=None, **fields_kwargs):
        fields = make_dict(fields, fields_kwargs)
        self._from_interface(fields)

    def __init_subclass__(cls):
        """When a child class is loaded into memory it will be saved into
        .orm_classes, this way every orm class knows about all the other orm
        classes, this is the method that makes that possible magically

        https://peps.python.org/pep-0487/
        """
        cls._message_classes[ReflectClass(cls).classpath] = cls

        # If we don't have a schema already set, let's create a class property
        # that will call `.create_schema` and then cache the value in the
        # class's `.schema` property
        if cls.schema is None:
            def cache_schema(cls):
                # cache the value so we don't need to generate it again
                cls.schema = cls.create_schema()
                return cls.schema

            cls.schema = classproperty(cache_schema)

    def __contains__(self, field_name: str) -> bool:
        v = getattr(self, field_name, typing.NoReturn)
        return v is not typing.NoReturn

    async def send(self, **kwargs) -> None:
        """send the message using the configured interface for this class

        :param **kwargs:
        :keyword delay_seconds: int, how many seconds before the message can
            be processed. The max value is interface specific, (eg, it can
            only be 900s max (15 minutes) per SQS docs)
        """
        queue_off = environ.DISABLED
        name = self.get_name()
        fields = self._to_interface()
        if queue_off:
            logger.warning(
                "DISABLED - Not sending %s message with %s fields to '%s'",
                self.__class__.__name__,
                fields,
                name,
            )

        else:
            logger.info(
                "Sending %s message with '%s' keys to '%s'",
                self.__class__.__name__,
                "', '".join(fields.keys()),
                name,
            )

            hydrate_fields = await self.interface.send(
                name=name,
                fields=fields,
                **kwargs,
            )
            # we mimic hydration
            self._from_interface(hydrate_fields)
            self._hydrate_fields = hydrate_fields

    @classmethod
    def get_name(cls) -> str:
        """This is what's used as the official queue name, it takes `.name`
        and combines it with the `MORP_PREFIX` environment variable

        :returns: str, the queue name
        """
        name = cls.queue_name
        if env_name := environ.PREFIX:
            name = f"{env_name}-{name}"
        return name

    @classmethod
    async def process(cls, count=0, **kwargs) -> None:
        """wait for messages to come in and handle them by calling the incoming
        message's `handle` method

        :example:
            # handle 10 messages by receiving them and calling `handle` on the
            # message instance
            Message.process(10)

        :param count: int, if you only want to handle N messages, pass in count
        :keyword name: the queue name, defaults to `.get_name`
        :keyword timeout: int, how long for the underlying `._recv` methods
            to wait before trying again
        :keyword ack_on_recv: bool, True if you want to acknowledge the
            message so it's removed from the queue on receive instead of
            on successful processing
        :param **kwargs: any other params will get passed to underlying 
            `._recv` methods
        """
        max_count = count
        count = 0
        while not max_count or count < max_count:
            count += 1
            logger.debug("Receiving %d/%s",
                count,
                max_count if max_count else "Infinity",
            )

            async with cls._recv(**kwargs) as m:
                r = m.handle()
                while inspect.iscoroutine(r):
                    r = await r

                if r is False:
                    raise ReleaseMessage()

    @classmethod
    @asynccontextmanager
    async def _recv(cls, **kwargs) -> AbstractAsyncContextManager[typing.Self]:
        """Internal context manager that receives a message to be processed

        Called from `.process` to receive messages and repeatedly calls
        `._recv_for` to actually receive a message

        :keyword timeout: int, how long to wait before yielding None
        """
        m = None
        # 20 is the max long polling timeout per Amazon
        kwargs.setdefault("timeout", 20)
        while not m:
            async with cls._recv_for(**kwargs) as m:
                if m:
                    yield m

    @classmethod
    @asynccontextmanager
    async def _recv_for(
        cls,
        timeout: int|float,
        **kwargs,
    ) -> AbstractAsyncContextManager[typing.Self]:
        """Internal context manager. Try and receive a message, return None
        if a message is not received within timeout

        :param timeout: float|int, how many seconds before yielding None
        :returns: generator[Message]
        """
        i = cls.interface
        name = kwargs.pop("name", cls.get_name())
        ack_on_recv = kwargs.pop("ack_on_recv", False)
        logger.debug("Waiting to receive on %s for %s seconds", name, timeout)

        async with i.connection(**kwargs) as connection:
            kwargs["connection"] = connection

            fields = await i.recv(name, timeout=timeout, **kwargs)
            if fields:
                try:
                    yield cls._from_recv(fields)

                except ReleaseMessage as e:
                    await i.release(
                        name,
                        fields,
                        delay_seconds=e.delay_seconds,
                        **kwargs
                    )

                except AckMessage as e:
                    await i.ack(name, fields, **kwargs)

                except Exception as e:
                    if ack_on_recv:
                        await i.ack(name, fields, **kwargs)

                    else:
                        await i.release(name, fields, **kwargs)

                    raise

                else:
                    await i.ack(name, fields, **kwargs)

            else:
                yield None

    @classmethod
    def create_schema(cls) -> dict[str, ReflectType]:
        schema = {}

        for field_name, field_type in typing.get_type_hints(cls).items():
            if (
                field_name.startswith("_")
                or typing.get_origin(field_type) is typing.ClassVar
            ):
                continue

            schema[field_name] = ReflectType(field_type)

        return schema

    @classmethod
    async def create(cls, *args, **kwargs) -> typing.Self:
        """create an instance of cls with the passed in fields and send it off

        Since this passed *args and **kwargs directly to .__init__, you can
        override the .__init__ method and customize it and this method will
        inherit the child class's changes. And the signature of this method
        should always match .__init__

        :param *args: list[Any], passed directly to .__init__
        :param **kwargs: dict[str, Any], passed directly to .__init__
        """
        connection = kwargs.pop("connection", None)
        instance = cls(*args, **kwargs)
        await instance.send(connection=connection)
        return instance

    @classmethod
    async def count(cls) -> int:
        """how many messages total (approximately) are in the whole message
        queue"""
        n = cls.get_name()
        return await cls.interface.count(n)

    @classmethod
    def _from_recv(cls, fields) -> typing.Self:
        """This is used by the interface to populate an instance with
        information received from the interface

        :param fields: InterfaceMessage, the message freshly received from
            the interface, see Interface.create_imessage()
        """
        message_class = cls
        if classpath := fields.get(cls._classpath_key):
            if classpath in cls._message_classes:
                message_class = cls._message_classes[classpath]

            else:
                cls_classpath = ReflectClass(cls).classpath
                if cls_classpath != classpath:
                    rn = ReflectName(classpath)
                    message_class = rn.get_class()

        instance = message_class()
        instance._from_interface(fields)
        instance._hydrate_fields = fields

        return instance

    def _to_interface(self) -> dict:
        """When sending a message to the interface this method will be called

        :returns: dict, the fields
        """
        fields = {}
        schema = self.schema
        for field_name, rt in schema.items():
            fields[field_name] = rt.cast(getattr(self, field_name))

        if self._classpath_key not in fields:
            fields[self._classpath_key] = ReflectClass(self).classpath

        return fields

    def _from_interface(self, fields) -> None:
        """When receiving a message from the interface this method will
        be called

        you can see it in action with `._from_recv`

        :param fields: dict, the fields received from the interface
        """
        for field_name in self.schema.keys():
            if field_name in fields:
                setattr(self, field_name, fields[field_name])

    async def handle(self) -> bool|None:
        """This method will be called from `.process` and can handle any
        processing of the message, it should be defined in the child classes

        :returns:
            - if False, the message will be released back to be processed
              again
            - any other value is considered a success and the message is
              acked/consumed
        """
        raise NotImplementedError()

