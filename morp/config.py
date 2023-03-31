# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import hashlib
import os
import base64

import dsnparse
from datatypes import ReflectClass
from datatypes import property as cachedproperty
from datatypes.config import Environ


from .compat import *


class Connection(object):
    """The base connection class, you will most likely always use DsnConnection"""
    name = ""
    """str, the name of this connection, handy when you have more than one used interface (eg, nsq)"""

    username = ""
    """str, the username (if needed)"""

    password = ""
    """str, the password (if needed)"""

    hosts = None
    """list, a list of (hostname, port) tuples"""

    path = None
    """str, the path (if applicable)"""

    interface_name = ""
    """str, full Interface class name -- the interface that will connect to the messaging backend"""

    options = None
    """dict, any other interface specific options you need"""

    @property
    def interface_class(self):
        interface_class = ReflectClass.get_class(self.interface_name)
        return interface_class

    @property
    def interface(self):
        interface_class = self.interface_class
        return interface_class(self)

    @property
    def serializer(self):
        serializer = self.options.get("serializer", "pickle")
        if serializer not in set(["pickle", "json"]):
            raise ValueError(f"Unknown serializer {serializer}")
        return serializer

    @cachedproperty(cached="_key")
    def key(self):
        """string -- an encryption key loaded from options['key'],
        it must be 32 bytes long so this makes sure it is"""
        key = self.options.get('key', "")
        if key:
            # Fernet key must be 32 url-safe base64-encoded bytes
            bs = ByteString(ByteString(key).sha256())
            key = base64.b64encode(bs[:32])
        return key

    def __init__(self, **kwargs):
        """
        set all the values by passing them into this constructor, any unrecognized kwargs get put into .options

        :Example:
            c = Connection(
                interface_name="...",
                hosts=[("host", port), ("host2", port2)],
                some_random_thing="foo"
            )

            print c.port # 5000
            print c.options # {"some_random_thing": "foo"}
        """
        self.options = kwargs.pop('options', {})
        self.hosts = []

        for key, val in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, val)
            else:
                self.options[key] = val

        self.options.setdefault('max_timeout', 3600) # 1 hour to process message
        self.options.setdefault('backoff_multiplier', 5) # failure backoff multiplier

    def get_netlocs(self, default_port):
        return ["{}:{}".format(h[0], default_port if h[1] is None else h[1]) for h in self.hosts]

    def get_option(self, key, default_val):
        return getattr(self.options, key, default_val)


class DsnConnection(Connection):
    """
    Create a connection object from a dsn in the form

        InterfaceName://username:password@host:port?opt1=val1&opt2=val2#connection_name

    example -- connect to amazon SQS

        morp.interface.sqs.SQS://AWS_ID:AWS_KEY@
    """
    def __init__(self, dsn):
        self.dsn = dsn
        d = self.parse(dsn)
        super().__init__(**d)

    def parse(self, dsn):
        d = {'options': {}, 'hosts': []}
        parser = dsnparse.parse(dsn)
        p = parser.fields
        p["dsn"] = parser.parser.dsn

        # get the scheme, which is actually our interface_name
        d['interface_name'] = self.normalize_scheme(p["scheme"])

        dsn_hosts = []
        if p["hostname"]:
            d['hosts'].append((p["hostname"], p.get('port', None)))

        d['options'] = p["query_params"] or {}
        d['name'] = p["fragment"]
        d['username'] = p["username"]
        d['password'] = p["password"]
        d["path"] = p["path"]

        return d

    def normalize_scheme(self, v):
        ret = v
        d = {
            "morp.interface.sqs:SQS": set(["sqs"]),
            "morp.interface.dropfile:Dropfile": set(["dropfile"]),
        }

        kv = v.lower()
        for interface_name, vals in d.items():
            if kv in vals:
                ret = interface_name
                break

        return ret


environ = Environ("MORP_") # Load any MORP_* environment variables

environ.setdefault('DISABLED', False, type=bool)
"""If set to 1 then messages won't actually be sent"""

environ.setdefault('PREFIX', '')
"""This prefix will be used to create the queue, see Message.get_name()"""

