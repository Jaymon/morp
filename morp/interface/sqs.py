# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
from contextlib import contextmanager
import re
import itertools
import base64

import boto3
from botocore.exceptions import ClientError

from ..compat import *
from .base import Interface


class Region(String):
    """Small wrapper that just makes sure the AWS region is valid"""
    def __new__(cls, region_name):
        if not region_name:
            session = boto3.Session()
            region_name = session.region_name
            if not region_name:
                raise ValueError("No region name found")

        return super().__new__(cls, region_name)

    @classmethod
    def names(cls):
        """Return all available regions for SQS"""
        session = boto3.Session()
        return session.get_available_regions("ec2")


class SQS(Interface):
    """wraps amazon's SQS to make it work with our generic interface

    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
    https://boto.readthedocs.org/en/latest/ref/sqs.html
    http://boto3.readthedocs.io/en/latest/guide/sqs.html
    http://michaelhallsmoore.com/blog/Python-Message-Queues-with-Amazon-Simple-Queue-Service
    http://aws.amazon.com/sqs/
    """
    _connection = None

    def _connect(self, connection_config):
        self.connection_config.options['vtimeout_max'] = 43200 # 12 hours max (from Amazon)

        region = Region(self.connection_config.options.get('region', ''))
        self.connection_config.options['region'] = region

        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
        # https://github.com/boto/boto3/issues/2360#issuecomment-761526845
        if arn := self.connection_config.options.get("arn", ""):
            sts = boto3.client(
                "sts",
                aws_access_key_id=connection_config.username,
                aws_secret_access_key=connection_config.password
            )

            role_info = sts.assume_role(
                RoleArn=arn,
                RoleSessionName="morp",
            )
            credentials = role_info["Credentials"]

            self._connection = boto3.resource(
                "sqs",
                region_name=region,
                aws_access_key_id=credentials["AccessKeyId"],
                aws_secret_access_key=credentials["SecretAccessKey"],
                aws_session_token=credentials["SessionToken"]
            )

            # we no longer need the sts client so clean it up
            self._close_client(sts)

        else:
            self._connection = boto3.resource(
                'sqs',
                region_name=region,
                aws_access_key_id=connection_config.username,
                aws_secret_access_key=connection_config.password
            )

        self.log("SQS connected to region {}", region)

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
                    if kwargs.get("create_queue", True):
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

    def fields_to_body(self, fields):
        """This base64 encodes the fields because SQS expects a string, not bytes

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/queue/send_message.html

        :param: dict, the fields to send to the backend
        :returns: str, the body, base64 encoded
        """
        body = super().fields_to_body(fields)
        return String(base64.b64encode(body))

    def _send(self, name, connection, body, **kwargs):
        with self.queue(name, connection) as q:
            delay_seconds = kwargs.get('delay_seconds', 0)
            if delay_seconds > 900:
                # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html
                self.log("delay_seconds({}) cannot be greater than 900", delay_seconds, level="warning")
                delay_seconds = 900

            # http://boto3.readthedocs.io/en/latest/reference/services/sqs.html#SQS.Queue.send_message
            receipt = q.send_message(MessageBody=body, DelaySeconds=delay_seconds)
            return receipt["MessageId"], receipt

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
        with self.queue(name, connection, create_queue=False) as q:
            if q:
                q.delete()

    def body_to_fields(self, body):
        """Before sending body to parent's body_to_fields() it will base64 decode it

        :param body: str, the body returned from the backend
        """
        return super().body_to_fields(base64.b64decode(body))

    def recv_to_fields(self, _id, body, raw):
        fields = super().recv_to_fields(_id, body, raw)

        # http://boto3.readthedocs.io/en/latest/reference/services/sqs.html#SQS.Queue.receive_messages
        fields["_count"] = int(raw.attributes.get('ApproximateReceiveCount', 1))
#         created_stamp = int(raw.attributes.get('SentTimestamp', 0.0)) / 1000.0
#         if created_stamp:
#             fields["_created"] = Datetime(created_stamp) 

        return fields

    def _recv(self, name, connection, **kwargs):
        timeout = kwargs.get('timeout', None)
        if timeout is not None:
            # http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html
            if timeout < 0 or timeout > 20:
                raise ValueError('timeout must be between 1 and 20')

        vtimeout = kwargs.get('vtimeout', None) # !!! I'm not sure this works
        with self.queue(name, connection) as q:
            _id = body = raw = None
            kwargs = {
                "MaxNumberOfMessages": 1,
                "AttributeNames": ["ApproximateReceiveCount", "SentTimestamp"],
            }

            if timeout:
                kwargs["WaitTimeSeconds"] = timeout
            if vtimeout:
                kwargs["VisibilityTimeout"] = min(
                    vtimeout,
                    self.connection_config.options['vtimeout_max']
                )

            msgs = q.receive_messages(**kwargs)
            if msgs:
                raw = msgs[0]
                body = raw.body
                _id = raw.message_id

            return _id, body, raw

    def _release(self, name, fields, connection, **kwargs):
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
                "Id": fields["_id"],
                "ReceiptHandle": fields["_raw"].receipt_handle,
                "VisibilityTimeout": delay_seconds
            }])

    def _ack(self, name, fields, connection, **kwargs):
        with self.queue(name, connection) as q:
            q.delete_messages(Entries=[
                {
                    'Id': fields["_id"],
                    'ReceiptHandle': fields["_raw"].receipt_handle,
                }
            ])
            # http://boto3.readthedocs.io/en/latest/reference/services/sqs.html#SQS.Message.delete

    def _is_client_error_match(self, e, codes):
        return e.response["Error"]["Code"] in codes

