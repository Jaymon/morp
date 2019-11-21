# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import hashlib
import os

import dsnparse

from .compat import *
from . import reflection


class Connection(object):
    """The base connection class, you will most likely always use DsnConnection"""
    name = ""
    """string -- the name of this connection, handy when you have more than one used interface (eg, nsq)"""

    username = ""
    """the username (if needed)"""

    password = ""
    """the password (if needed)"""

    hosts = None
    """list -- a list of (hostname, port) tuples"""

    interface_name = ""
    """string -- full Interface class name -- the interface that will connect to the messaging backend"""

    options = None
    """dict -- any other interface specific options you need"""

    @property
    def interface_class(self):
        interface_class = reflection.get_class(self.interface_name)
        return interface_class

    @property
    def interface(self):
        interface_class = self.interface_class
        return interface_class(self)

    @property
    def key(self):
        """string -- an encryption key loaded from options['key'],
        it must be 32 bytes long so this makes sure it is"""
        if not hasattr(self, '_key'):
            key = self.options.get('key', "")
            if key:
                # !!! deprecated 2019-11-16, key shouldn't be a file anymore
                if os.path.isfile(key):
                    with open(key, 'r') as f:
                        key = f.read().strip()

            # key must be 32 characters long
            self._key = ByteString(key).sha256() if key else ""

        return self._key

    def __init__(self, **kwargs):
        """
        set all the values by passing them into this constructor, any unrecognized kwargs get put into .options

        example --
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
        d = self.parse(dsn)
        super(DsnConnection, self).__init__(**d)

    @classmethod
    def parse(cls, dsn):
        d = {'options': {}, 'hosts': []}
        p = dsnparse.ParseResult.parse(dsn)

        # get the scheme, which is actually our interface_name
        d['interface_name'] = cls.normalize_scheme(p["scheme"])

        dsn_hosts = []
        if "hostname" in p:
            d['hosts'].append((p["hostname"], p.get('port', None)))

        d['options'] = p["query"] or {}

        if "username" in p:
            d['username'] = p["username"]

        if "password" in p:
            d['password'] = p["password"]

        if "fragment" in p:
            d['name'] = p["fragment"]

        return d

    @classmethod
    def normalize_scheme(cls, v):
        ret = v
        d = {
            "morp.interface.sqs.SQS": set(["sqs"]),
        }

        kv = v.lower()
        for interface_name, vals in d.items():
            if kv in vals:
                ret = interface_name
                break

        return ret

