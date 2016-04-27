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
        self.log("SQS connected to region {}", region)
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
            # http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html
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

    def _release(self, name, interface_msg, connection, **kwargs):
        with self.queue(name, connection) as q:
            # http://stackoverflow.com/questions/14404007/release-a-message-back-to-sqs
            # http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/AboutVT.html
            # When you [change] a message's visibility timeout, the new timeout applies
            # only to that particular receipt of the message. ChangeMessageVisibility
            # does not affect the timeout for the queue or later receipts of the message.
            # If for some reason you don't delete the message and receive it again,
            # its visibility timeout is the original value set for the queue.
            q.change_message_visibility_batch([(interface_msg.raw_msg, 0)])

    def _ack(self, name, interface_msg, connection, **kwargs):
        with self.queue(name, connection) as q:
            q.delete_message(interface_msg.raw_msg)

