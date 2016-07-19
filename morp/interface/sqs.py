from __future__ import absolute_import
from contextlib import contextmanager
import datetime

import boto3
from botocore.exceptions import ClientError

from . import Interface, InterfaceMessage


class SQSMessage(InterfaceMessage):
    """Thin wrapper around the InterfaceMessage to account for SQS keeping internal
    count and created values"""
    def depart(self):
        return self.fields

    def populate(self, fields):
        if not fields: fields = {}
        self.fields = fields.get("fields", fields)
        self._count = 0
        self._created = None

        # http://boto3.readthedocs.io/en/latest/reference/services/sqs.html#SQS.Queue.receive_messages
        if self.raw:
            self._count = int(self.raw.attributes.get('ApproximateReceiveCount', 1))
            created_stamp = int(self.raw.attributes.get('SentTimestamp', 0.0)) / 1000.0
            if created_stamp:
                self._created = datetime.datetime.fromtimestamp(created_stamp) 


class SQS(Interface):
    """wraps amazon's SQS to make it work with our generic interface

    http://boto3.readthedocs.io/en/latest/guide/sqs.html
    http://michaelhallsmoore.com/blog/Python-Message-Queues-with-Amazon-Simple-Queue-Service
    http://aws.amazon.com/sqs/
    """
    _connection = None

    message_class = SQSMessage

    def _connect(self, connection_config):
        self.connection_config.options['vtimeout_max'] = 43200 # 12 hours max (from Amazon)
        self.connection_config.options.setdefault('region', 'us-west-1')

        region = self.connection_config.options.get('region')
        self.log("SQS connected to region {}", region)
        self._connection = boto3.resource(
            'sqs',
            region_name=region,
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
        # http://boto3.readthedocs.io/en/latest/reference/services/sqs.html#SQS.Queue
        try:
            q = None

            try:
                q = connection.get_queue_by_name(QueueName=name)
                yield q

            except ClientError as e:
                if self._is_client_error_match(e, "AWS.SimpleQueueService.NonExistentQueue"):
                    attrs = {}
                    # we use max_timeout here because we will release the message
                    # sooner according to our release algo but on exceptional error
                    # let's use our global max setting
                    vtimeout = self.connection_config.options.get('max_timeout')
                    if vtimeout:
                        attrs["VisibilityTimeout"] = str(min(vtimeout, 43200))

                    q = connection.create_queue(
                        QueueName=name,
                        Attributes=attrs 
                    )

                    yield q

                else:
                    raise

        except Exception as e:
            self.raise_error(e)

    def _send(self, name, body, connection, **kwargs):
        with self.queue(name, connection) as q:
            delay_seconds = kwargs.get('delay_seconds', 0)
            # http://boto3.readthedocs.io/en/latest/reference/services/sqs.html#SQS.Queue.send_message
            q.send_message(MessageBody=body, DelaySeconds=delay_seconds)

    def _count(self, name, connection, **kwargs):
        ret = 0
        with self.queue(name, connection) as q:
            ret = int(q.attributes.get('ApproximateNumberOfMessages', 0))
        return ret

    def _clear(self, name, connection, **kwargs):
        with self.queue(name, connection) as q:
            try:
                q.purge()

            except ClientError as e:
                if not self._is_client_error_match(e, "AWS.SimpleQueueService.PurgeQueueInProgress"):
                    raise

    def _recv(self, name, connection, **kwargs):
        # if timeout isn't set then this will return immediately with no values
        timeout = kwargs.get('timeout', None)
        if timeout is not None:
            # http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html
            if timeout < 0 or timeout > 20:
                raise ValueError('timeout must be between 1 and 20')

        vtimeout = None # kwargs.get('vtimeout', None)
        with self.queue(name, connection) as q:
            body = ''
            raw = None

            kwargs = {
                "MaxNumberOfMessages": 1,
                "AttributeNames": ["ApproximateReceiveCount", "SentTimestamp"],
            }

            if timeout:
                kwargs["WaitTimeSeconds"] = timeout
            if vtimeout:
                kwargs["VisibilityTimeout"] = min(vtimeout, 43200)

            msgs = q.receive_messages(**kwargs)
            if msgs:
                raw = msgs[0]
                body = raw.body

            return body, raw

    def _release(self, name, interface_msg, connection, **kwargs):
        with self.queue(name, connection) as q:
            # http://stackoverflow.com/questions/14404007/release-a-message-back-to-sqs
            # http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/AboutVT.html
            # When you [change] a message's visibility timeout, the new timeout applies
            # only to that particular receipt of the message. ChangeMessageVisibility
            # does not affect the timeout for the queue or later receipts of the message.
            # If for some reason you don't delete the message and receive it again,
            # its visibility timeout is the original value set for the queue.
            delay_seconds = kwargs.get('delay_seconds', 0)
            q.change_message_visibility_batch(Entries=[{
                "Id": interface_msg.raw.message_id,
                "ReceiptHandle": interface_msg.raw.receipt_handle,
                "VisibilityTimeout": delay_seconds
            }])

    def _ack(self, name, interface_msg, connection, **kwargs):
        with self.queue(name, connection) as q:
            q.delete_messages(Entries=[
                {
                    'Id': interface_msg.raw.message_id,
                    'ReceiptHandle': interface_msg.raw.receipt_handle,
                }
            ])
            # http://boto3.readthedocs.io/en/latest/reference/services/sqs.html#SQS.Message.delete

    def _is_client_error_match(self, e, code):
        return e.response["Error"]["Code"] == code

