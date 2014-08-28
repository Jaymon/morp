from __future__ import absolute_import # needed to import official nsq
from contextlib import contextmanager

import boto.sqs
from boto.sqs.jsonmessage import JSONMessage

from . import Interface

class SQS(Interface):

    _connection = None

    def _connect(self, connection_config):
        region = connection_config.options.get('region', 'us-west-1')
        self._connection = boto.sqs.connect_to_region(
            region,
            aws_access_key_id=connection_config.username,
            aws_secret_access_key=connection_config.password
        )

    def get_connection(self):
        return self._connection

    def _close(self):
        """I can't find a close in the docs, so not doing anything

        https://boto.readthedocs.org/en/latest/ref/sqs.html
        """
        self._connection = None

    @contextmanager
    def queue(self, name, connection, **kwargs):
        try:
            q = connection.create_queue(
                name,
                self.connection_config.options.get('read_lock', 360)
            )
            q.set_message_class(JSONMessage)

            yield q

        except Exception as e:
            self.raise_error(e)

    def _send(self, name, fields, connection, **kwargs):
        with self.queue(name, connection) as q:
            m = q.new_message(body=fields)
            q.write(m)

    def _count(self, name, connection, **kwargs):
        ret = 0
        with self.queue(name, connection) as q:
            ret = q.count()
        return ret

    def _clear(self, name, connection, **kwargs):
        with self.queue(name, connection) as q:
            q.clear()

    def _recv(self, name, connection, **kwargs):
        with self.queue(name, connection) as q:
            raw_msg = q.get_messages(1)[0]
            return raw_msg.get_body(), raw_msg

    def _ack(self, name, interface_msg, connection, **kwargs):
        with self.queue(name, connection) as q:
            q.delete_message(interface_msg.raw_msg)

