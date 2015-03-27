from __future__ import absolute_import # needed to import official nsq
from contextlib import contextmanager

import boto.sqs
from boto.sqs.jsonmessage import JSONMessage

from . import Interface

class SQS(Interface):
    """wraps amazon's SQS to make it work with our generic interface

    https://boto.readthedocs.org/en/latest/ref/sqs.html
    https://boto.readthedocs.org/en/latest/sqs_tut.html
    http://michaelhallsmoore.com/blog/Python-Message-Queues-with-Amazon-Simple-Queue-Service
    http://aws.amazon.com/sqs/
    """
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
            q = connection.lookup(name)
            if q is None:
                q = connection.create_queue(
                    name,
                    self.connection_config.options.get('vtimeout', 360)
                )

            #q.set_message_class(JSONMessage)
            yield q

        except Exception as e:
            self.raise_error(e)

    def _send(self, name, body, connection, **kwargs):
        with self.queue(name, connection) as q:
            delay_seconds = kwargs.get('delay_seconds', None)
            m = q.new_message(body=body)
            q.write(m, delay_seconds)

    def _count(self, name, connection, **kwargs):
        ret = 0
        with self.queue(name, connection) as q:
            ret = q.count()
        return ret

    def _clear(self, name, connection, **kwargs):
        with self.queue(name, connection) as q:
            q.clear()

    def _recv(self, name, connection, **kwargs):
        timeout = kwargs.get('timeout', None)
        if timeout is not None:
            if timeout < 0 or timeout > 20:
                raise ValueError('timeout must be between 1 and 20')

        vtimeout = kwargs.get('vtimeout', None)
        with self.queue(name, connection) as q:
            body = ''
            raw_msg = {}
            msgs = q.get_messages(
                1,
                wait_time_seconds=timeout,
                visibility_timeout=vtimeout
            )
            if msgs:
                raw_msg = msgs[0]
                body = raw_msg.get_body()

            return body, raw_msg

    def _ack(self, name, interface_msg, connection, **kwargs):
        with self.queue(name, connection) as q:
            q.delete_message(interface_msg.raw_msg)

