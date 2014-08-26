from __future__ import absolute_import # needed to import official nsq
import urllib2
import urllib
import json

from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message

from . import Interface

class SQS(Interface):

    default_host = "127.0.0.1"

    default_port = 4150

    default_channel = 'default'

    _connection = None

    def _connect(self, connection_config):
        self._connection = SQSConnection(
            connection_config.username,
            connection_config.password
        )


    def _close(self):
        self.connection.s.close()

    def assure(self):
        """handle any things that need to be done before a query can be performed"""
        self.connect()

    def _send(self, name, msg_str):
        self.connection.send(nsq.pub(name, msg_str))

    def _consume(self, message_names):
        readers = []
        lookupd_poll_interval = self.connection_config.get_option('lookupd_poll_interval', 1)
        hosts = self.get_http_hosts()
        if not message_names:
            message_names = self.get_topics()

        for mn in message_names:
            topic, channels = self.normalize_topic(mn)
            for channel in channels:
                self.log("start consume {}.{}", topic, channel)
                readers.append(nsq.Reader(
                    message_handler=self.handle,
                    lookupd_http_addresses=hosts,
                    topic=topic,
                    channel=channel,
                    lookupd_poll_interval=15
                ))

        #nsq.run()
        nsq.tornado.ioloop.IOLoop.instance().start()

    def handle(self, interface_msg):
        """callback for the nsq ioloop"""
        r = True
        message = self.denormalize_message(interface_msg.body)
        message.interface_msg = interface_msg
        try:
            r = message.handle()
            if r is None:
                r = True
        except Exception, e:
            self.log(e)
            r = False

        return r

    def normalize_topic(self, message_name):
        """we allow topic.channel_name to be passed in, this will split message_name into its topic, channels"""
        topic = message_name
        channels = set([self.default_channel])
        bits = filter(None, message_name.split('.'))
        if bits:
            topic = bits[0]
            channels |= set(bits[1:])

        return topic, channels

    def get_http_hosts(self):
        """
        get the hosts that will be used

        TODO -- make this work with ssl
        """
        return ["http://{}".format(netloc) for netloc in self.connection_config.get_netlocs(4161)]

    def get_topics(self):
        """get all the topics from all the lookupd instances"""
        topics = set()
        for url in self.get_http_hosts():
            url = "{}/topics".format(url)
            response = urllib2.urlopen(url)
            if response.code == 200:
                body = json.loads(response.read())
                topics |= set(body['data']['topics'])

        return topics


