# -*- coding: utf-8 -*-
from contextlib import contextmanager
import fcntl
import errno
import time
import uuid

from datatypes import (
    Dirpath,
    Filepath,
)

from ..compat import *
from .base import Interface


class Dropfile(Interface):
    """Dropfile interface using local files as messages, great for quick prototyping
    or passing messages from the frontend to the backend on the same machine
    """
    _connection = None

    @contextmanager
    def queue(self, name, connection, **kwargs):
        yield connection.child_dir(name, touch=True)

    def _connect(self, connection_config):
        self._connection = Dirpath(connection_config.path, "morp", "queue", touch=True)

    def get_connection(self):
        return self._connection

    def _close(self):
        pass

    def _send(self, name, connection, body, **kwargs):
        with self.queue(name, connection) as queue:
            now = time.time_ns()
            _id = uuid.uuid4().hex

            if delay_seconds := kwargs.get('delay_seconds', 0):
                now += (delay_seconds * 1000000000)

            message = queue.child_file(f"{now}-{_id}-1.txt")
            message.write_bytes(body)
            return _id, message

    def _count(self, name, connection, **kwargs):
        with self.queue(name, connection) as queue:
            return queue.files().count()

    def recv_to_fields(self, _id, body, raw):
        fields = super().recv_to_fields(_id, body, raw)
        fields["_count"] = raw._count
        fields["_body"] = body
        #fields["_created"] = raw.stat[9]
        return fields

    def _recv(self, name, connection, **kwargs):
        _id = body = raw = None
        timeout = kwargs.get('timeout', None) or 0.0
        count = 0.0

        with self.queue(name, connection) as queue:
            while count <= timeout:
                now = time.time_ns()
                for message in queue.files().sort():
                    parts = message.fileroot.split("-")
                    then = int(parts[0])
                    if now > then:
                        fp = message.open("rb+")
                        try:
                            fcntl.flock(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)

                            body = fp.read()
                            if body:
                                _id = parts[1]
                                message.fp = fp
                                message._count = int(parts[2])
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

        return _id, body, raw

    def _ack(self, name, connection, fields, **kwargs):
        message = fields["_raw"]
        self._cleanup(message.fp, message)

    def _release(self, name, connection, fields, **kwargs):
        delay_seconds = kwargs.get('delay_seconds', 0)

        _id = fields["_id"]
        message = fields["_raw"]
        body = fields["_body"]
        fp = message.fp

        if delay_seconds:
            now = time.time_ns() + (delay_seconds * 1000000000)

            # let's copy the file body to the future and then delete the old message,
            # sadly, because we've got a lock on the file we can't move or copy it,
            # so we're just going to create a new file
            count = fields["_count"] + 1
            dest = Filepath(message.dirname, f"{now}-{_id}-{count}.txt")
            dest.write_bytes(body)
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

