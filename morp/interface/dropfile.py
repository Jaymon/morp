# -*- coding: utf-8 -*-
from contextlib import contextmanager
import fcntl
import errno
import datetime
import time

from datatypes import (
    Dirpath,
    Filepath,
    Datetime,
)

from ..compat import *
from .base import Interface, InterfaceMessage


class DropfileMessage(InterfaceMessage):
    @property
    def _id(self):
        return self._fileroot

    def from_interface(self, body):
        super().from_interface(body)

        if self.raw:
            parts = self.raw.fileroot.split("-")
            self._fileroot = parts[0]
            self._created = Datetime(self._fileroot)
            self._count = 1
            if len(parts) > 1:
                self._count = int(parts[1])


class Dropfile(Interface):
    """Dropfile interface using local files as messages, great for quick prototyping
    or passing messages from the frontend to the backend on the same machine
    """
    _connection = None

    message_class = DropfileMessage

    @contextmanager
    def queue(self, name, connection, **kwargs):
        yield connection.child_dir(name, touch=True)

    def _connect(self, connection_config):
        self._connection = Dirpath(connection_config.path, touch=True)

    def get_connection(self):
        return self._connection

    def _close(self):
        pass

    def _send(self, name, connection, body, **kwargs):
        with self.queue(name, connection) as queue:
            dt = Datetime()

            if delay_seconds := kwargs.get('delay_seconds', 0):
                dt = dt + datetime.timedelta(seconds=delay_seconds)

            message = queue.child_file(f"{dt.timestamp()}-1.txt")
            message.write_bytes(body)

    def _count(self, name, connection, **kwargs):
        with self.queue(name, connection) as queue:
            return queue.files().count()

    def _recv(self, name, connection, **kwargs):
        body = ""
        raw = None
        timeout = kwargs.get('timeout', None) or 0.0
        count = 0.0

        with self.queue(name, connection) as queue:
            while count <= timeout:
                now = Datetime()
                for message in queue.files().sort():
                    then = Datetime(message.fileroot.split("-")[0])
                    if now > then:
                        fp = message.open("rb+")
                        try:
                            fcntl.flock(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)

                            body = fp.read()
                            if body:
                                message.fp = fp
                                raw = message
                                break

                            else:
                                # looks like another process got to this message
                                # first, so try and clean it up
                                self._cleanup(fp, message, truncate=False)

                        except OSError as e:
                            pass

                if body:
                    break

                else:
                    count += 0.1
                    if count < timeout:
                        time.sleep(0.1)

        return body, raw

    def _ack(self, name, connection, imessage, **kwargs):
        self._cleanup(imessage.raw.fp, imessage.raw)

    def _release(self, name, connection, imessage, **kwargs):
        delay_seconds = kwargs.get('delay_seconds', 0)

        message = imessage.raw
        fp = message.fp

        if delay_seconds:
            now = Datetime()
            then = now + datetime.timedelta(seconds=delay_seconds)

            # let's move the file to the future and then delete the old message
            count = imessage._count + 1
            dest = Filepath(message.dirname, f"{then.timestamp()}-{count}.txt")
            dest.write_bytes(imessage.body)
            self._cleanup(fp, message)

        else:
            # release the message back into the queue
            self._cleanup(fp, message, truncate=False, delete=False)

    def _clear(self, name, connection, **kwargs):
        with self.queue(name, connection) as queue:
            queue.clear()

    def _delete(self, name, connection, **kwargs):
        with self.queue(name, connection) as queue:
            queue.delete()

    def _cleanup(self, fp, message, **kwargs):
        # clear the message so other get() requests will move on (this is the
        # one thing we can do with an exclusive lock to tell other processes
        # we have already looked at the file, I wish we could delete under an 
        # exclusive lock)
        if kwargs.get("truncate", True):
            fp.truncate()

        fcntl.flock(fp, fcntl.LOCK_UN)
        fp.close()

        if kwargs.get("delete", True):
            message.delete()

