# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
from contextlib import contextmanager
import datetime
import re
import itertools

import boto3
from botocore.exceptions import ClientError

from ..compat import *
from . import Interface, InterfaceMessage


class Region(String):
    def __new__(cls, region_name):
        if not region_name:
            session = boto3.Session()
            region_name = session.region_name
            if not region_name:
                raise ValueError("No region name found")

        return super(Region, cls).__new__(cls, region_name)

    @classmethod
    def names(cls):
        session = boto3.Session()
        return session.get_available_regions("ec2")


class SQSMessage(InterfaceMessage):
    """Thin wrapper around the InterfaceMessage to account for SQS keeping internal
    count and created values"""
    @property
    def _id(self):
        return self.raw.message_id

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

    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
    https://boto.readthedocs.org/en/latest/ref/sqs.html
    http://boto3.readthedocs.io/en/latest/guide/sqs.html
    http://michaelhallsmoore.com/blog/Python-Message-Queues-with-Amazon-Simple-Queue-Service
    http://aws.amazon.com/sqs/
    """
    _connection = None

    message_class = SQSMessage

    def _connect(self, connection_config):
        self.connection_config.options['vtimeout_max'] = 43200 # 12 hours max (from Amazon)
        self.connection_config.options['region'] = Region(self.connection_config.options.get('region', ''))

        region = self.connection_config.options.get('region')
        self.log("SQS connected to region {}", region)
        self._connection = boto3.resource(
        #self._connection = boto3.client(
            'sqs',
            region_name=region,
            aws_access_key_id=connection_config.username,
            aws_secret_access_key=connection_config.password
        )

    def get_connection(self):
        return self._connection

    def _close(self):
        """closes out the client and gets rid of connection"""
        if self._connection:
            client = self._connection.meta.client
            self._close_client(client)

        self._connection = None

    def _close_client(self, client):
        """closes open sessions on client

        this code comes from: 
            https://github.com/boto/botocore/pull/1810

        it closes the connections to fix unclosed connection warnings in py3

        Specifically, this warning:

            ResourceWarning: unclosed <ssl.SSLSocket ...
            ResourceWarning: Enable tracemalloc to get the object allocation tracebac

        see also:
            https://github.com/boto/boto3/issues/454
            https://github.com/boto/botocore/pull/1231

        this might also be a good/alternate solution for this problem:
            https://github.com/boto/boto3/issues/454#issuecomment-335614919

        :param client: an amazon services client whose sessions will be closed
        """
        client._endpoint.http_session._manager.clear()
        for manager in client._endpoint.http_session._proxy_managers.values():
            manager.close()

    def get_attrs(self, **kwargs):
        attrs = {}
        options = self.connection_config.options

        # we use max_timeout here because we will release the message
        # sooner according to our release algo but on exceptional error
        # let's use our global max setting
        vtimeout = options.get('max_timeout')
        if vtimeout:
            # if not string fails with:
            # Invalid type for parameter Attributes.VisibilityTimeout, value: 3600,
            # type: <type 'int'>, valid types: <type 'basestring'>
            attrs["VisibilityTimeout"] = String(min(vtimeout, options["vtimeout_max"]))

        for k, v in itertools.chain(options.items(), kwargs.items()):
            if re.match("^[A-Z][a-zA-Z]+$", k):
                attrs[k] = v

        return attrs

    @contextmanager
    def queue(self, name, connection, **kwargs):
        # http://boto3.readthedocs.io/en/latest/reference/services/sqs.html#SQS.Queue
        try:
            q = None

            try:
                q = connection.get_queue_by_name(QueueName=name)
                yield q

            except ClientError as e:
                if self._is_client_error_match(e, ["AWS.SimpleQueueService.NonExistentQueue"]):
                    attrs = self.get_attrs(**kwargs)
                    q = connection.create_queue(
                        QueueName=name,
                        Attributes=attrs 
                    )

                    yield q

                else:
                    raise

        except Exception as e:
            self.raise_error(e)

        finally:
            if q:
                self._close_client(q.meta.client)

    def _send(self, name, body, connection, **kwargs):
        with self.queue(name, connection) as q:
            delay_seconds = kwargs.get('delay_seconds', 0)
            if delay_seconds > 900:
                # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html
                self.log("delay_seconds({}) cannot be greater than 900", delay_seconds, level="warn")
                delay_seconds = 900

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
                if not self._is_client_error_match(e, ["AWS.SimpleQueueService.PurgeQueueInProgress"]):
                    raise

    def _delete(self, name, connection, **kwargs):
        # !!! this doesn't work because it tries to create the queue
        #with self.queue(name, connection) as q:
        #    q.delete()

        client = None
        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.get_queue_url
            # https://stackoverflow.com/q/38581465/5006
            client = connection.meta.client
            queue_url = client.get_queue_url(QueueName=name)["QueueUrl"]
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.delete_queue
            client.delete_queue(QueueUrl=queue_url)

        except ClientError as e:
            if not self._is_client_error_match(e, [
                "AWS.SimpleQueueService.QueueDeletedRecently",
                "AWS.SimpleQueueService.NonExistentQueue",
            ]):
                raise

        finally:
            if client:
                self._close_client(client)

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

    def _is_client_error_match(self, e, codes):
        return e.response["Error"]["Code"] in codes

