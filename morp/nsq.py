import json
import urllib2

import nsq

import morp

class Nsq(object):

    def send(name, message=None, **message_kwargs):
        if not message: message = {}
        message.update(message_kwargs)
        data = json.dumps(message)
        c = morp.get_connection(name, type="send")[0]
        # TODO maybe -- async this call either with vanilla threads or use tornado so that this
        # returns straight away
        url = "http://{}:{}/put?topic={}".format(c.host, c.port, name)
        r = urllib2.urlopen(url, data)
        # TODO -- add support for ephemeral messages

        if r.code != 200:
            raise RuntimeError("failed to send message to {} using {}".format(name, url))

        return True

    def recv(*names):

        if names:
            for name in names:
                connections = morp.get_connection(name, type="recv")

                urls = ["http://{}:{}".format(c.host, c.port) for c in connections]
                r = nsq.Reader(
                    lookupd_http_addresses=urls,
                    topic="foo",
                    channel="test",
                    #message_handler=handler
                    lookupd_poll_interval=15
                )


