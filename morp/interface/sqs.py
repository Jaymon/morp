from __future__ import absolute_import
from contextlib import contextmanager

import boto3
from botocore.exceptions import ClientError

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
        self._connection = boto3.resource(
            'sqs',
            region_name=region,
            aws_access_key_id=connection_config.username,
            aws_secret_access_key=connection_config.password
        )
#         self._connection = sqs.connect_to_region(
#             region,
#             aws_access_key_id=connection_config.username,
#             aws_secret_access_key=connection_config.password
#         )

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
            try:
                q = connection.get_queue_by_name(QueueName=name)

            except Exception as e:
                # TODO -- this needs to be tightened to the correct error
                pout.v(e)
                q = None

            finally:
                if q is None:
                    attrs = {}
                    vtimeout = self.connection_config.options.get('max_timeout')
                    if vtimeout:
                        attrs["VisibilityTimeout"] = str(vtimeout)

                    q = connection.create_queue(
                        QueueName=name,
                        Attributes=attrs 
                    )

            yield q

        except Exception as e:
            self.raise_error(e)

    def _send(self, name, body, connection, **kwargs):
        with self.queue(name, connection) as q:
            delay_seconds = kwargs.get('delay_seconds', 0)
            # http://boto3.readthedocs.io/en/latest/reference/services/sqs.html#SQS.Queue.send_message
            q.send_message(MessageBody=body, DelaySeconds=delay_seconds)
            #m = q.new_message(body=body)
            #q.write(m, delay_seconds)

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
                if e.response["Error"]["Code"] != "AWS.SimpleQueueService.PurgeQueueInProgress":
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

            kwargs = {"MaxNumberOfMessages": 1}
            if timeout:
                kwargs["WaitTimeSeconds"] = timeout
            if vtimeout:
                kwargs["VisibilityTimeout"] = timeout

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
            #interface_msg.raw.delete()

