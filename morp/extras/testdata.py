import logging

from testdata.base import TestData

from ..message import Message
from ..interface import get_interfaces


logger = logging.getLogger(__name__)


class MessageData(TestData):
    def get_async_cleanups(self):
        return [
            (self.unsafe_delete_messages, [], {}),
            (self.close_message_interfaces, [], {}),
        ]

    async def close_message_interfaces(self):
        """Close down all the globally created interfaces

        This, along with the unsafe_* methods are designed to be used in
        project testing
        """
        for inter in get_interfaces().values():
            await inter.close()

    async def unsafe_delete_messages(self):
        """This will delete all the messages from all the queues

        .. note:: You'll want to only call the method in the right
            environments as this really will delete all the messages in
            whatever message queues it has interfaces for
        """
        seen = set()
        for message_class in Message._message_classes.values():
            name = message_class.get_name()
            if name not in seen:
                seen.add(name)
                try:
                    inter = message_class.interface
                    await inter.unsafe_delete(name)

                except KeyError:
                    logger.warning("Could not clear queue: %s", name)

